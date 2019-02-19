service info {
    /*
     Returns the content of this file so that clients can adjust
     themselves to the servers rpc.thift version.
     */
    string thrift_content(),
    /*
     Returns the output of `nvidia-smi -q` command.
     */
    string nvidia_smi_query(),
}
