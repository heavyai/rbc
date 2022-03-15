import os
import pytest
from rbc.tests import omnisci_fixture
from rbc.external import external


@pytest.fixture(scope='module')
def omnisci():
    for o in omnisci_fixture(globals(), minimal_version=(5, 6), debug=not True):
        yield o


@pytest.fixture(scope='module')
def boston_house_prices(omnisci):
    # Upload Boston house prices to server, notice that we collect all
    # row values expect the last one (MEDV) to a FLOAT array.

    csv_file = os.path.join(os.path.dirname(__file__), 'boston_house_prices.csv')
    data0 = []
    medv0 = []
    for i, line in enumerate(open(csv_file).readlines()):
        line = line.strip().replace(' ', '')
        if i == 0:
            header = line.split(',')
        else:
            row = list(map(float, line.split(',')))
            assert len(row) == len(header)
            data0.append(row[:-1])
            medv0.append(row[-1])
    table_name = f'{omnisci.table_name}bhp'

    omnisci.sql_execute(f'DROP TABLE IF EXISTS {table_name}')
    omnisci.sql_execute(f'CREATE TABLE IF NOT EXISTS {table_name} (data FLOAT[], medv FLOAT);')
    omnisci.load_table_columnar(table_name, data=data0, medv=medv0)

    yield omnisci

    omnisci.sql_execute(f'DROP TABLE IF EXISTS {table_name}')


def test_boston_house_prices(omnisci, boston_house_prices):
    if omnisci.compiler is None:
        pytest.skip('test requires C/C++ to LLVM IR compiler')

    treelite = pytest.importorskip("treelite")
    xgboost = pytest.importorskip("xgboost")

    device = 'cpu'
    import numpy as np
    import tempfile

    # Get training data from server:
    table_name = f'{omnisci.table_name}bhp'

    descr, result = omnisci.sql_execute(
        f'SELECT rowid, data, medv FROM {table_name} ORDER BY rowid LIMIT 50')
    result = list(result)
    medv = np.array([medv for _, data, medv in result])
    data = np.array([data for _, data, medv in result])
    assert len(medv) == 50

    # Train model using xgboost
    dtrain = xgboost.DMatrix(data, label=medv)
    params = {'max_depth': 3,
              'eta': 1,
              'objective': 'reg:squarederror',
              'eval_metric': 'rmse'}
    bst = xgboost.train(params, dtrain, len(medv), [(dtrain, 'train')])

    # Compile model to C
    working_dir = tempfile.mkdtemp()
    model = treelite.Model.from_xgboost(bst)
    model.compile(working_dir)
    model_c = open(os.path.join(working_dir, 'main.c')).read()
    # The C model implements
    #  float predict(union Entry* data, int pred_margin)
    # but we wrap it using
    model_c += '''
#ifdef __cplusplus
extern "C" {
#endif
    float predict_float(float* data, int pred_margin) {
      return predict((union Entry*)data, pred_margin);
    }
#ifdef __cplusplus
}
#endif
    '''
    predict_float = external('float predict_float(float*, int32)')
    # to make UDF construction easier. Notice that predict_float can
    # be now called from a UDF.

    # Define predict function as UDF. Notice that the xgboost null
    # values are different from omniscidb null values, so we'll remap
    # before calling predict_float:
    null_value = np.int32(-1).view(np.float32)

    @omnisci('float(float[], int32)', devices=[device])
    def predict(data, pred_margin):
        for i in range(len(data)):
            if data.is_null(i):
                data[i] = null_value
        return predict_float(data.ptr(), pred_margin)

    # Compile C model to LLVM IR. In future, we might want this
    # compilation to happen in the server side as the client might not
    # have clang compiler installed.
    model_llvmir = omnisci.compiler(model_c, flags=['-I' + working_dir])

    # RBC will link_in the LLVM IR module
    omnisci.user_defined_llvm_ir[device] = model_llvmir

    # Call predict on data in the server:
    descr, result = omnisci.sql_execute('SELECT rowid, predict(data, 2) FROM'
                                        f' {table_name} ORDER BY rowid')
    result = list(result)
    predict_medv = np.array([r[1] for r in result])

    # predict on the first 50 elements should be close to training labels
    error = abs(predict_medv[:len(medv)] - medv).max()/len(medv)
    assert error < 1e-4, error
