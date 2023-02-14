; ModuleID = 'rbc.irtools.compile_to_IR'
source_filename = "<string>"
target triple = "x86_64-unknown-linux-gnu"

%Struct.MemInfo.11 = type { i64, i8*, i8*, i8*, i64, i8* }

@__nrt_global_var = local_unnamed_addr global i64 0
@printf_format = internal constant [4 x i8] c"%*d\00"
@printf_format.2 = internal constant [4 x i8] c"%*d\00"
@printf_format.4 = internal constant [4 x i8] c"%*d\00"
@printf_format.5 = internal constant [27 x i8] c"[NRT] bytes=%zu -> ptr=%p\0A\00"
@printf_format.6 = internal constant [4 x i8] c"%*d\00"
@printf_format.8 = internal constant [4 x i8] c"%*d\00"
@printf_format.9 = internal constant [19 x i8] c"[NRT] NRT_Free %p\0A\00"
@printf_format.38 = internal constant [4 x i8] c"%*d\00"
@printf_format.40 = internal constant [4 x i8] c"%*d\00"
@printf_format.41 = internal constant [29 x i8] c"[NRT] mi=%p data=%p size=%d\0A\00"
@printf_format.70 = internal constant [4 x i8] c"%*d\00"
@printf_format.72 = internal constant [4 x i8] c"%*d\00"
@printf_format.73 = internal constant [22 x i8] c"[NRT] base=%p out=%p\0A\00"
@printf_format.14 = internal constant [4 x i8] c"%*d\00"
@printf_format.16 = internal constant [4 x i8] c"%*d\00"
@printf_format.17 = internal constant [24 x i8] c"[NRT] data=%p size=%zu\0A\00"
@printf_format.75 = internal constant [4 x i8] c"%*d\00"
@printf_format.77 = internal constant [4 x i8] c"%*d\00"
@printf_format.78 = internal constant [23 x i8] c"[NRT] ptr=%p, info=%p\0A\00"
@printf_format.18 = internal constant [4 x i8] c"%*d\00"
@printf_format.20 = internal constant [4 x i8] c"%*d\00"
@printf_format.21 = internal constant [24 x i8] c"[NRT] data=%p size=%zu\0A\00"
@printf_format.22 = internal constant [4 x i8] c"%*d\00"
@printf_format.24 = internal constant [4 x i8] c"%*d\00"
@printf_format.30 = internal constant [4 x i8] c"%*d\00"
@printf_format.32 = internal constant [4 x i8] c"%*d\00"
@printf_format.33 = internal constant [16 x i8] c"[NRT] data: %p\0A\00"
@printf_format.42 = internal constant [4 x i8] c"%*d\00"
@printf_format.44 = internal constant [4 x i8] c"%*d\00"
@printf_format.45 = internal constant [50 x i8] c"[NRT] mi=%p data=%p size=%d dtor=%p dtor_info=%p\0A\00"
@printf_format.46 = internal constant [4 x i8] c"%*d\00"
@printf_format.48 = internal constant [4 x i8] c"%*d\00"
@printf_format.49 = internal constant [39 x i8] c"[NRT] size=%zu -> meminfo=%p, data=%p\0A\00"
@printf_format.50 = internal constant [4 x i8] c"%*d\00"
@printf_format.52 = internal constant [4 x i8] c"%*d\00"
@printf_format.53 = internal constant [16 x i8] c"[NRT] size: %d\0A\00"
@printf_format.54 = internal constant [4 x i8] c"%*d\00"
@printf_format.56 = internal constant [4 x i8] c"%*d\00"
@printf_format.57 = internal constant [30 x i8] c"[NRT] %p size=%zu -> data=%p\0A\00"
@printf_format.58 = internal constant [4 x i8] c"%*d\00"
@_ZN08NumbaEnv3rbc5tests7heavydb8test_nrt15test_set_simple12_3clocals_3e2fnB2v1B44c8tJTC_2fWQA9wW1DkAz0Pj1skAdT4gkkUlYBZmgA_3dE73TextEncodingNonePointer_5bSTRUCT__cPW3WptrLW2WszV5Vbool8W7Wis_nullK_5d_2a = common local_unnamed_addr global i8* null
@.bytes.-6635387936450191170 = internal unnamed_addr constant [3 x i8] c"ab\00"
@printf_format.79 = internal constant [43 x i8] c"rbc: fn__cpu_0 failed with status code %i\0A\00"
@_ZN08NumbaEnv5numba7cpython7unicode15unicode_getitem12_3clocals_3e12getitem_charB2v2B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_typey = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7unicode11unicode_len12_3clocals_3e8len_implB2v3B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7unicode17normalize_str_idxB2v4B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dEyx27omitted_28default_3dTrue_29 = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7unicode15_get_code_pointB2v5B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typed = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7unicode18_codepoint_to_kindB2v6B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dEx = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7unicode19_codepoint_is_asciiB2v7B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dEx = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7unicode13_empty_stringB2v8B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dExxb = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7unicode19_kind_to_byte_widthB2v9B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dEx = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7unicode15_set_code_pointB3v10B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typexj = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7unicode15_set_code_pointB3v11B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typexx = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7unicode11unicode_len12_3clocals_3e8len_implB3v12B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7unicode10unicode_eq12_3clocals_3e7eq_implB3v33B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type12unicode_type = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7unicode11unicode_str12_3clocals_3e12_3clambda_3eB3v34B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7unicode11_cmp_regionB3v35B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex12unicode_typexx = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7unicode15_get_code_pointB3v36B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7hashing13hash_overload12_3clocals_3e4implB3v13B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7hashing12unicode_hash12_3clocals_3e4implB3v14B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7unicode19_kind_to_byte_widthB3v15B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dEi = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7hashing13_Py_HashBytesB3v16B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE7void_2ax = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7hashing14process_returnB3v17B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dEx = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7hashing21_impl_load_hashsecret12_3clocals_3e3impB3v19B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE36Literal_5bstr_5d_28djbx33a_suffix_29 = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7hashing14process_returnB3v20B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dEy = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7hashing21_impl_load_hashsecret12_3clocals_3e3impB3v22B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE32Literal_5bstr_5d_28siphash_k0_29 = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7hashing21_impl_load_hashsecret12_3clocals_3e3impB3v24B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE32Literal_5bstr_5d_28siphash_k1_29 = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7hashing10_siphash24B3v25B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dEyy7void_2ax = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7hashing13_DOUBLE_ROUNDB3v26B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dEyyyy = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7hashing11_HALF_ROUNDB3v27B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dEyyyyxx = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7hashing7_ROTATEB3v28B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dEyy = common local_unnamed_addr global i8* null
@_ZN08NumbaEnv5numba7cpython7hashing7_ROTATEB3v29B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dEyx = common local_unnamed_addr global i8* null
@_numba_hashsecret_siphash_k0 = external local_unnamed_addr global i64
@_numba_hashsecret_siphash_k1 = external local_unnamed_addr global i64
@str = private unnamed_addr constant [19 x i8] c"[NRT] NRT_Allocate\00", align 1
@str.88 = private unnamed_addr constant [28 x i8] c"[NRT] NRT_Allocate_External\00", align 1
@str.89 = private unnamed_addr constant [15 x i8] c"[NRT] NRT_Free\00", align 1
@str.91 = private unnamed_addr constant [36 x i8] c"[NRT] nrt_allocate_meminfo_and_data\00", align 1
@str.92 = private unnamed_addr constant [23 x i8] c"[NRT] NRT_MemInfo_init\00", align 1
@str.93 = private unnamed_addr constant [29 x i8] c"[NRT] NRT_MemInfo_alloc_dtor\00", align 1
@str.94 = private unnamed_addr constant [31 x i8] c"[NRT] nrt_internal_custom_dtor\00", align 1
@str.95 = private unnamed_addr constant [34 x i8] c"[NRT] NRT_MemInfo_alloc_dtor_safe\00", align 1
@str.96 = private unnamed_addr constant [29 x i8] c"[NRT] NRT_MemInfo_alloc_safe\00", align 1
@str.97 = private unnamed_addr constant [60 x i8] c"[NRT] NRT_MemInfo_alloc_safe -> NRT_MemInfo_alloc_dtor_safe\00", align 1
@str.101 = private unnamed_addr constant [28 x i8] c"[NRT] NRT_MemInfo_data_fast\00", align 1
@str.102 = private unnamed_addr constant [22 x i8] c"[NRT] NRT_MemInfo_new\00", align 1
@str.103 = private unnamed_addr constant [30 x i8] c"[NRT] NRT_MemInfo_new_varsize\00", align 1
@str.104 = private unnamed_addr constant [35 x i8] c"[NRT] NRT_MemInfo_new_varsize_dtor\00", align 1
@str.105 = private unnamed_addr constant [32 x i8] c"[NRT] NRT_MemInfo_varsize_alloc\00", align 1
@str.106 = private unnamed_addr constant [31 x i8] c"[NRT] NRT_MemInfo_varsize_free\00", align 1

; Function Attrs: nofree noinline nounwind
define private fastcc i8* @NRT_Allocate(i64 %.1) unnamed_addr #0 {
entry:
  tail call fastcc void @nrt_debug_incr()
  %.4 = load i64, i64* @__nrt_global_var, align 8
  %.6 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format, i64 0, i64 0), i64 %.4, i64 %.4)
  %puts = tail call i32 @puts(i8* nonnull dereferenceable(1) getelementptr inbounds ([19 x i8], [19 x i8]* @str, i64 0, i64 0))
  %.9 = tail call fastcc i8* @NRT_Allocate_External(i64 %.1)
  tail call fastcc void @nrt_debug_decr()
  ret i8* %.9
}

; Function Attrs: nofree noinline norecurse nounwind
define private fastcc void @nrt_debug_incr() unnamed_addr #1 {
entry:
  %.2 = load i64, i64* @__nrt_global_var, align 8
  %.3 = add i64 %.2, 4
  store i64 %.3, i64* @__nrt_global_var, align 8
  ret void
}

; Function Attrs: nofree nounwind
declare i32 @printf(i8* nocapture readonly, ...) local_unnamed_addr #2

; Function Attrs: nofree noinline nounwind
define private fastcc i8* @NRT_Allocate_External(i64 %.1) unnamed_addr #0 {
entry:
  tail call fastcc void @nrt_debug_incr()
  %.5 = load i64, i64* @__nrt_global_var, align 8
  %.7 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.2, i64 0, i64 0), i64 %.5, i64 %.5)
  %puts = tail call i32 @puts(i8* nonnull dereferenceable(1) getelementptr inbounds ([28 x i8], [28 x i8]* @str.88, i64 0, i64 0))
  %.10 = tail call i8* @malloc(i64 %.1)
  %.11 = load i64, i64* @__nrt_global_var, align 8
  %.13 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.4, i64 0, i64 0), i64 %.11, i64 %.11)
  %.15 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([27 x i8], [27 x i8]* @printf_format.5, i64 0, i64 0), i64 %.1, i8* %.10)
  tail call fastcc void @nrt_debug_decr()
  ret i8* %.10
}

; Function Attrs: nofree noinline norecurse nounwind
define private fastcc void @nrt_debug_decr() unnamed_addr #1 {
entry:
  %.2 = load i64, i64* @__nrt_global_var, align 8
  %.3 = add i64 %.2, -4
  store i64 %.3, i64* @__nrt_global_var, align 8
  ret void
}

; Function Attrs: nofree nounwind
declare noalias i8* @malloc(i64) local_unnamed_addr #2

; Function Attrs: nofree noinline nounwind
define private fastcc void @NRT_Free(i8* %.1) unnamed_addr #0 {
entry:
  tail call fastcc void @nrt_debug_incr()
  %.4 = load i64, i64* @__nrt_global_var, align 8
  %.6 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.6, i64 0, i64 0), i64 %.4, i64 %.4)
  %puts = tail call i32 @puts(i8* nonnull dereferenceable(1) getelementptr inbounds ([15 x i8], [15 x i8]* @str.89, i64 0, i64 0))
  %.9 = load i64, i64* @__nrt_global_var, align 8
  %.11 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.8, i64 0, i64 0), i64 %.9, i64 %.9)
  %.13 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([19 x i8], [19 x i8]* @printf_format.9, i64 0, i64 0), i8* %.1)
  tail call fastcc void @nrt_debug_decr()
  ret void
}

; Function Attrs: nofree noinline nounwind
define private fastcc nonnull i8* @nrt_allocate_meminfo_and_data(i8** nocapture %.2) unnamed_addr #0 {
entry:
  tail call fastcc void @nrt_debug_incr()
  %.6 = load i64, i64* @__nrt_global_var, align 8
  %.8 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.70, i64 0, i64 0), i64 %.6, i64 %.6)
  %puts = tail call i32 @puts(i8* nonnull dereferenceable(1) getelementptr inbounds ([36 x i8], [36 x i8]* @str.91, i64 0, i64 0))
  %.12 = tail call fastcc i8* @NRT_Allocate_External(i64 50)
  store i8* %.12, i8** %.2, align 8
  %.14 = getelementptr inbounds i8, i8* %.12, i64 48
  %.15 = load i64, i64* @__nrt_global_var, align 8
  %.17 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.72, i64 0, i64 0), i64 %.15, i64 %.15)
  %.19 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([22 x i8], [22 x i8]* @printf_format.73, i64 0, i64 0), i8* %.12, i8* nonnull %.14)
  tail call fastcc void @nrt_debug_decr()
  ret i8* %.14
}

; Function Attrs: nofree noinline nounwind
define private fastcc void @NRT_MemInfo_init(%Struct.MemInfo.11* %.1, i8* %.2, i64 %.3, i8* %.4) unnamed_addr #0 {
entry:
  tail call fastcc void @nrt_debug_incr()
  %.9 = load i64, i64* @__nrt_global_var, align 8
  %.11 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.38, i64 0, i64 0), i64 %.9, i64 %.9)
  %puts = tail call i32 @puts(i8* nonnull dereferenceable(1) getelementptr inbounds ([23 x i8], [23 x i8]* @str.92, i64 0, i64 0))
  %.14 = getelementptr inbounds %Struct.MemInfo.11, %Struct.MemInfo.11* %.1, i64 0, i32 0
  store i64 1, i64* %.14, align 4
  %.16 = getelementptr inbounds %Struct.MemInfo.11, %Struct.MemInfo.11* %.1, i64 0, i32 1
  store i8* %.4, i8** %.16, align 8
  %.18 = getelementptr inbounds %Struct.MemInfo.11, %Struct.MemInfo.11* %.1, i64 0, i32 2
  store i8* null, i8** %.18, align 8
  %.20 = getelementptr inbounds %Struct.MemInfo.11, %Struct.MemInfo.11* %.1, i64 0, i32 3
  store i8* %.2, i8** %.20, align 8
  %.22 = getelementptr inbounds %Struct.MemInfo.11, %Struct.MemInfo.11* %.1, i64 0, i32 4
  store i64 %.3, i64* %.22, align 4
  %.24 = getelementptr inbounds %Struct.MemInfo.11, %Struct.MemInfo.11* %.1, i64 0, i32 5
  store i8* null, i8** %.24, align 8
  %.26 = load i64, i64* @__nrt_global_var, align 8
  %.28 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.40, i64 0, i64 0), i64 %.26, i64 %.26)
  %.30 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([29 x i8], [29 x i8]* @printf_format.41, i64 0, i64 0), %Struct.MemInfo.11* %.1, i8* %.2, i64 %.3)
  tail call fastcc void @nrt_debug_decr()
  ret void
}

; Function Attrs: nofree noinline nounwind
define private fastcc %Struct.MemInfo.11* @NRT_MemInfo_alloc_dtor() unnamed_addr #0 {
entry:
  tail call fastcc void @nrt_debug_incr()
  %.5 = load i64, i64* @__nrt_global_var, align 8
  %.7 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.14, i64 0, i64 0), i64 %.5, i64 %.5)
  %puts = tail call i32 @puts(i8* nonnull dereferenceable(1) getelementptr inbounds ([29 x i8], [29 x i8]* @str.93, i64 0, i64 0))
  %mi = alloca %Struct.MemInfo.11*, align 8
  %mi_cast = bitcast %Struct.MemInfo.11** %mi to i8**
  %data = call fastcc i8* @nrt_allocate_meminfo_and_data(i8** nonnull %mi_cast)
  %.10 = load i64, i64* @__nrt_global_var, align 8
  %.12 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.16, i64 0, i64 0), i64 %.10, i64 %.10)
  %.14 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([24 x i8], [24 x i8]* @printf_format.17, i64 0, i64 0), i8* nonnull %data, i64 2)
  %.16 = load %Struct.MemInfo.11*, %Struct.MemInfo.11** %mi, align 8
  tail call fastcc void @NRT_MemInfo_init(%Struct.MemInfo.11* %.16, i8* nonnull %data, i64 2, i8* bitcast (void (i8*, i64, i8*)* @nrt_internal_custom_dtor to i8*))
  tail call fastcc void @nrt_debug_decr()
  ret %Struct.MemInfo.11* %.16
}

; Function Attrs: noinline
define private void @nrt_internal_custom_dtor(i8* %.1, i64 %.2, i8* %.3) #3 {
entry:
  tail call fastcc void @nrt_debug_incr()
  %.6 = load i64, i64* @__nrt_global_var, align 8
  %.8 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.75, i64 0, i64 0), i64 %.6, i64 %.6)
  %puts = tail call i32 @puts(i8* nonnull dereferenceable(1) getelementptr inbounds ([31 x i8], [31 x i8]* @str.94, i64 0, i64 0))
  %.12 = load i64, i64* @__nrt_global_var, align 8
  %.14 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.77, i64 0, i64 0), i64 %.12, i64 %.12)
  %.16 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([23 x i8], [23 x i8]* @printf_format.78, i64 0, i64 0), i8* %.1, i8* %.3)
  %.17.not = icmp eq i8* %.3, null
  br i1 %.17.not, label %entry.endif, label %entry.if, !prof !16

entry.if:                                         ; preds = %entry
  %.11 = bitcast i8* %.3 to void (i8*, i64, i8*)*
  tail call void %.11(i8* %.1, i64 %.2, i8* null)
  br label %entry.endif

entry.endif:                                      ; preds = %entry, %entry.if
  tail call fastcc void @nrt_debug_decr()
  ret void
}

; Function Attrs: nofree noinline nounwind
define private fastcc %Struct.MemInfo.11* @NRT_MemInfo_alloc_dtor_safe() unnamed_addr #0 {
entry:
  tail call fastcc void @nrt_debug_incr()
  %.5 = load i64, i64* @__nrt_global_var, align 8
  %.7 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.18, i64 0, i64 0), i64 %.5, i64 %.5)
  %puts = tail call i32 @puts(i8* nonnull dereferenceable(1) getelementptr inbounds ([34 x i8], [34 x i8]* @str.95, i64 0, i64 0))
  %data = tail call fastcc %Struct.MemInfo.11* @NRT_MemInfo_alloc_dtor()
  %.10 = load i64, i64* @__nrt_global_var, align 8
  %.12 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.20, i64 0, i64 0), i64 %.10, i64 %.10)
  %.14 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([24 x i8], [24 x i8]* @printf_format.21, i64 0, i64 0), %Struct.MemInfo.11* %data, i64 2)
  tail call fastcc void @nrt_debug_decr()
  ret %Struct.MemInfo.11* %data
}

; Function Attrs: nofree noinline nounwind
define private fastcc %Struct.MemInfo.11* @NRT_MemInfo_alloc_safe() unnamed_addr #0 {
entry:
  tail call fastcc void @nrt_debug_incr()
  %.4 = load i64, i64* @__nrt_global_var, align 8
  %.6 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.22, i64 0, i64 0), i64 %.4, i64 %.4)
  %puts = tail call i32 @puts(i8* nonnull dereferenceable(1) getelementptr inbounds ([29 x i8], [29 x i8]* @str.96, i64 0, i64 0))
  %.9 = load i64, i64* @__nrt_global_var, align 8
  %.11 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.24, i64 0, i64 0), i64 %.9, i64 %.9)
  %puts1 = tail call i32 @puts(i8* nonnull dereferenceable(1) getelementptr inbounds ([60 x i8], [60 x i8]* @str.97, i64 0, i64 0))
  %.14 = tail call fastcc %Struct.MemInfo.11* @NRT_MemInfo_alloc_dtor_safe()
  tail call fastcc void @nrt_debug_decr()
  ret %Struct.MemInfo.11* %.14
}

; Function Attrs: nofree noinline nounwind
define private fastcc i8* @NRT_MemInfo_data_fast(i8* nocapture readonly %.1) unnamed_addr #0 {
entry:
  tail call fastcc void @nrt_debug_incr()
  %.4 = load i64, i64* @__nrt_global_var, align 8
  %.6 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.30, i64 0, i64 0), i64 %.4, i64 %.4)
  %puts = tail call i32 @puts(i8* nonnull dereferenceable(1) getelementptr inbounds ([28 x i8], [28 x i8]* @str.101, i64 0, i64 0))
  %.9 = getelementptr inbounds i8, i8* %.1, i64 24
  %0 = bitcast i8* %.9 to i8**
  %data = load i8*, i8** %0, align 8
  %.10 = load i64, i64* @__nrt_global_var, align 8
  %.12 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.32, i64 0, i64 0), i64 %.10, i64 %.10)
  %.14 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([16 x i8], [16 x i8]* @printf_format.33, i64 0, i64 0), i8* %data)
  tail call fastcc void @nrt_debug_decr()
  ret i8* %data
}

; Function Attrs: nofree noinline nounwind
define private fastcc %Struct.MemInfo.11* @NRT_MemInfo_new(i8* %.1) unnamed_addr #0 {
entry:
  tail call fastcc void @nrt_debug_incr()
  %.7 = load i64, i64* @__nrt_global_var, align 8
  %.9 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.42, i64 0, i64 0), i64 %.7, i64 %.7)
  %puts = tail call i32 @puts(i8* nonnull dereferenceable(1) getelementptr inbounds ([22 x i8], [22 x i8]* @str.102, i64 0, i64 0))
  %.12 = tail call fastcc i8* @NRT_Allocate(i64 48)
  %mi = bitcast i8* %.12 to %Struct.MemInfo.11*
  %.13.not = icmp eq i8* %.12, null
  br i1 %.13.not, label %entry.endif, label %entry.if, !prof !16

entry.if:                                         ; preds = %entry
  %.15 = load i64, i64* @__nrt_global_var, align 8
  %.17 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.44, i64 0, i64 0), i64 %.15, i64 %.15)
  %.19 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([50 x i8], [50 x i8]* @printf_format.45, i64 0, i64 0), i8* nonnull %.12, i8* %.1, i64 936, i8* null, i8* null)
  tail call fastcc void @NRT_MemInfo_init(%Struct.MemInfo.11* nonnull %mi, i8* %.1, i64 936, i8* null)
  br label %entry.endif

entry.endif:                                      ; preds = %entry, %entry.if
  tail call fastcc void @nrt_debug_decr()
  ret %Struct.MemInfo.11* %mi
}

; Function Attrs: nofree noinline nounwind
define private fastcc %Struct.MemInfo.11* @NRT_MemInfo_new_varsize() unnamed_addr #0 {
entry:
  tail call fastcc void @nrt_debug_incr()
  %.4 = load i64, i64* @__nrt_global_var, align 8
  %.6 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.46, i64 0, i64 0), i64 %.4, i64 %.4)
  %puts = tail call i32 @puts(i8* nonnull dereferenceable(1) getelementptr inbounds ([30 x i8], [30 x i8]* @str.103, i64 0, i64 0))
  %.9 = tail call fastcc i8* @NRT_Allocate(i64 936)
  %mi = tail call fastcc %Struct.MemInfo.11* @NRT_MemInfo_new(i8* %.9)
  %.10 = load i64, i64* @__nrt_global_var, align 8
  %.12 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.48, i64 0, i64 0), i64 %.10, i64 %.10)
  %.14 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([39 x i8], [39 x i8]* @printf_format.49, i64 0, i64 0), i64 936, %Struct.MemInfo.11* %mi, i8* %.9)
  tail call fastcc void @nrt_debug_decr()
  ret %Struct.MemInfo.11* %mi
}

; Function Attrs: nofree noinline nounwind
define private fastcc %Struct.MemInfo.11* @NRT_MemInfo_new_varsize_dtor() unnamed_addr #0 {
entry:
  tail call fastcc void @nrt_debug_incr()
  %.5 = load i64, i64* @__nrt_global_var, align 8
  %.7 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.50, i64 0, i64 0), i64 %.5, i64 %.5)
  %puts = tail call i32 @puts(i8* nonnull dereferenceable(1) getelementptr inbounds ([35 x i8], [35 x i8]* @str.104, i64 0, i64 0))
  %.10 = load i64, i64* @__nrt_global_var, align 8
  %.12 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.52, i64 0, i64 0), i64 %.10, i64 %.10)
  %.14 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([16 x i8], [16 x i8]* @printf_format.53, i64 0, i64 0), i64 936)
  %.15 = tail call fastcc %Struct.MemInfo.11* @NRT_MemInfo_new_varsize()
  %.16.not = icmp eq %Struct.MemInfo.11* %.15, null
  br i1 %.16.not, label %entry.endif, label %entry.if, !prof !16

entry.if:                                         ; preds = %entry
  %.18 = getelementptr inbounds %Struct.MemInfo.11, %Struct.MemInfo.11* %.15, i64 0, i32 1
  store i8* bitcast (void (i8*, i64, i8*)* @.dtor.set.unicode_type to i8*), i8** %.18, align 8
  br label %entry.endif

entry.endif:                                      ; preds = %entry, %entry.if
  tail call fastcc void @nrt_debug_decr()
  ret %Struct.MemInfo.11* %.15
}

; Function Attrs: nofree noinline nounwind
define private fastcc i8* @NRT_MemInfo_varsize_alloc(%Struct.MemInfo.11* %.1, i64 %.2) unnamed_addr #0 {
entry:
  tail call fastcc void @nrt_debug_incr()
  %.5 = load i64, i64* @__nrt_global_var, align 8
  %.7 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.54, i64 0, i64 0), i64 %.5, i64 %.5)
  %puts = tail call i32 @puts(i8* nonnull dereferenceable(1) getelementptr inbounds ([32 x i8], [32 x i8]* @str.105, i64 0, i64 0))
  %.10 = tail call fastcc i8* @NRT_Allocate(i64 %.2)
  %.11 = getelementptr inbounds %Struct.MemInfo.11, %Struct.MemInfo.11* %.1, i64 0, i32 3
  store i8* %.10, i8** %.11, align 8
  %.13 = getelementptr inbounds %Struct.MemInfo.11, %Struct.MemInfo.11* %.1, i64 0, i32 4
  store i64 %.2, i64* %.13, align 4
  %.15 = load i64, i64* @__nrt_global_var, align 8
  %.17 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.56, i64 0, i64 0), i64 %.15, i64 %.15)
  %.19 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([30 x i8], [30 x i8]* @printf_format.57, i64 0, i64 0), %Struct.MemInfo.11* %.1, i64 %.2, i8* %.10)
  tail call fastcc void @nrt_debug_decr()
  ret i8* %.10
}

; Function Attrs: nofree noinline nounwind
define private fastcc void @NRT_MemInfo_varsize_free(%Struct.MemInfo.11* nocapture %.1, i8* %.2) unnamed_addr #0 {
entry:
  tail call fastcc void @nrt_debug_incr()
  %.5 = load i64, i64* @__nrt_global_var, align 8
  %.7 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([4 x i8], [4 x i8]* @printf_format.58, i64 0, i64 0), i64 %.5, i64 %.5)
  %puts = tail call i32 @puts(i8* nonnull dereferenceable(1) getelementptr inbounds ([31 x i8], [31 x i8]* @str.106, i64 0, i64 0))
  tail call fastcc void @NRT_Free(i8* %.2)
  %.11 = getelementptr inbounds %Struct.MemInfo.11, %Struct.MemInfo.11* %.1, i64 0, i32 3
  %data = load i8*, i8** %.11, align 8
  %.12 = icmp eq i8* %data, %.2
  br i1 %.12, label %entry.if, label %entry.endif

entry.if:                                         ; preds = %entry
  store i8* null, i8** %.11, align 8
  br label %entry.endif

entry.endif:                                      ; preds = %entry.if, %entry
  tail call fastcc void @nrt_debug_decr()
  ret void
}

; Function Attrs: norecurse nounwind readnone
define private void @.dtor.set.unicode_type(i8* nocapture %.1, i64 %.2, i8* nocapture %.3) #4 {
.5:
  ret void
}

; Function Attrs: argmemonly nounwind willreturn writeonly
declare void @llvm.memset.p0i8.i64(i8* nocapture writeonly, i8, i64, i1 immarg) #5

; Function Attrs: nounwind readnone speculatable willreturn
declare { i64, i1 } @llvm.smul.with.overflow.i64(i64, i64) #6

; Function Attrs: nounwind readnone speculatable willreturn
declare { i64, i1 } @llvm.sadd.with.overflow.i64(i64, i64) #6

; Function Attrs: nounwind
define i32 @fn__cpu_0({ i8*, i64, i8 }* nocapture readnone %.1) local_unnamed_addr #7 {
entry:
  %.230.i29.i = alloca i64, align 8
  %.230.i.i = alloca i64, align 8
  %.46.i = tail call fastcc %Struct.MemInfo.11* @NRT_MemInfo_new_varsize_dtor() #7
  %0 = bitcast %Struct.MemInfo.11* %.46.i to i8*
  %.47.i = icmp eq %Struct.MemInfo.11* %.46.i, null
  br i1 %.47.i, label %entry.if, label %B0.endif.endif.endif.i, !prof !16

B0.endif.endif.endif.i:                           ; preds = %entry
  %.57.i = tail call fastcc i8* @NRT_MemInfo_data_fast(i8* nonnull %0) #7, !noalias !17
  %1 = getelementptr inbounds i8, i8* %.57.i, i64 32
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 1 dereferenceable(936) %1, i8 -1, i64 904, i1 false) #7, !noalias !17
  %.65.i = getelementptr inbounds i8, i8* %.57.i, i64 24
  %2 = bitcast i8* %.65.i to i64*
  store i64 0, i64* %2, align 8, !noalias !17
  %.68.i = getelementptr inbounds i8, i8* %.57.i, i64 16
  %3 = bitcast i8* %.68.i to i64*
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %.57.i, i8 0, i64 16, i1 false) #7, !noalias !17
  store i64 15, i64* %3, align 8, !noalias !17
  %4 = bitcast i64* %.230.i29.i to i8*
  %5 = bitcast i64* %.230.i.i to i8*
  br label %for_iter.body.endif.if.i

for_iter.body.endif.if.i:                         ; preds = %lookup.end.endif.i, %B0.endif.endif.endif.i
  %.131.i = phi i1 [ true, %B0.endif.endif.endif.i ], [ false, %lookup.end.endif.i ]
  %.85.0345.i = phi i64 [ 0, %B0.endif.endif.endif.i ], [ 1, %lookup.end.endif.i ]
  %.37.i.i.i = getelementptr [3 x i8], [3 x i8]* @.bytes.-6635387936450191170, i64 0, i64 %.85.0345.i
  %.38.i.i.i = load i8, i8* %.37.i.i.i, align 1, !noalias !21
  %.49.i.i.i = tail call fastcc %Struct.MemInfo.11* @NRT_MemInfo_alloc_safe() #7
  %.50.i.i.i = icmp eq %Struct.MemInfo.11* %.49.i.i.i, null
  br i1 %.50.i.i.i, label %entry.if, label %B104.i.i.i.i, !prof !16

B104.i.i.i.i:                                     ; preds = %for_iter.body.endif.if.i
  %.5.i.i.i.i = getelementptr %Struct.MemInfo.11, %Struct.MemInfo.11* %.49.i.i.i, i64 0, i32 3
  %.6.i.i.i.i = load i8*, i8** %.5.i.i.i.i, align 8, !noalias !25
  %.41.i.i.i.i = getelementptr i8, i8* %.6.i.i.i.i, i64 1
  store i8 0, i8* %.41.i.i.i.i, align 1, !noalias !29
  store i8 %.38.i.i.i, i8* %.6.i.i.i.i, align 1, !noalias !36
  %.198.i = tail call fastcc i8* @NRT_MemInfo_data_fast(i8* nonnull %0) #7, !noalias !17
  %.200.i = getelementptr inbounds i8, i8* %.198.i, i64 40
  %6 = bitcast i8* %.200.i to { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }*
  %.5.i6.i.i.i.i = load i64, i64* @_numba_hashsecret_siphash_k0, align 8, !noalias !40
  %.5.i3.i.i.i.i = load i64, i64* @_numba_hashsecret_siphash_k1, align 8, !noalias !52
  %.20.i.i = xor i64 %.5.i6.i.i.i.i, 8317987319222330741
  %.27.i.i = xor i64 %.5.i3.i.i.i.i, 7237128888997146477
  %.34.i.i = xor i64 %.5.i6.i.i.i.i, 7816392313619706465
  %.41.i.i = xor i64 %.5.i3.i.i.i.i, 8387220255154660723
  %.483.i.i = load i8, i8* %.6.i.i.i.i, align 1, !noalias !55
  %.486.i.i = zext i8 %.483.i.i to i64
  %.499.i.i = or i64 %.486.i.i, 72057594037927936
  %.505.i.i = xor i64 %.41.i.i, %.499.i.i
  %.10.i.i113.i.i = add i64 %.27.i.i, %.20.i.i
  %.11.i.i114.i.i = add i64 %.505.i.i, %.34.i.i
  %.8.i2.i.i115.i.i = lshr i64 %.10.i.i113.i.i, 32
  %.6.i1.i.i116.i.i = shl i64 %.10.i.i113.i.i, 32
  %.9.i3.i.i117.i.i = or i64 %.8.i2.i.i115.i.i, %.6.i1.i.i116.i.i
  %.8.i6.i.i118.i.i = lshr i64 %.505.i.i, 48
  %.6.i4.i.i119.i.i = shl i64 %.505.i.i, 16
  %.9.i7.i.i120.i.i = or i64 %.8.i6.i.i118.i.i, %.6.i4.i.i119.i.i
  %.63.i.i121.i.i = xor i64 %.9.i7.i.i120.i.i, %.11.i.i114.i.i
  %.8.i.i.i122.i.i = lshr i64 %.27.i.i, 51
  %.6.i.i.i123.i.i = shl i64 %.27.i.i, 13
  %.9.i.i.i124.i.i = or i64 %.8.i.i.i122.i.i, %.6.i.i.i123.i.i
  %.38.i.i125.i.i = xor i64 %.9.i.i.i124.i.i, %.10.i.i113.i.i
  %.10.i35.i126.i.i = add i64 %.11.i.i114.i.i, %.38.i.i125.i.i
  %.11.i36.i127.i.i = add i64 %.63.i.i121.i.i, %.9.i3.i.i117.i.i
  %.8.i2.i37.i128.i.i = lshr i64 %.10.i35.i126.i.i, 32
  %.6.i1.i38.i129.i.i = shl i64 %.10.i35.i126.i.i, 32
  %.9.i3.i39.i130.i.i = or i64 %.8.i2.i37.i128.i.i, %.6.i1.i38.i129.i.i
  %.8.i6.i40.i131.i.i = lshr i64 %.63.i.i121.i.i, 43
  %.6.i4.i41.i132.i.i = shl i64 %.63.i.i121.i.i, 21
  %.9.i7.i42.i133.i.i = or i64 %.8.i6.i40.i131.i.i, %.6.i4.i41.i132.i.i
  %.63.i43.i134.i.i = xor i64 %.9.i7.i42.i133.i.i, %.11.i36.i127.i.i
  %.8.i.i44.i135.i.i = lshr i64 %.38.i.i125.i.i, 47
  %.6.i.i45.i136.i.i = shl i64 %.38.i.i125.i.i, 17
  %.9.i.i46.i137.i.i = or i64 %.8.i.i44.i135.i.i, %.6.i.i45.i136.i.i
  %.38.i47.i138.i.i = xor i64 %.10.i35.i126.i.i, %.9.i.i46.i137.i.i
  %.10.i18.i139.i.i = add i64 %.11.i36.i127.i.i, %.38.i47.i138.i.i
  %.11.i19.i140.i.i = add i64 %.63.i43.i134.i.i, %.9.i3.i39.i130.i.i
  %.8.i2.i20.i141.i.i = lshr i64 %.10.i18.i139.i.i, 32
  %.6.i1.i21.i142.i.i = shl i64 %.10.i18.i139.i.i, 32
  %.9.i3.i22.i143.i.i = or i64 %.8.i2.i20.i141.i.i, %.6.i1.i21.i142.i.i
  %.8.i6.i23.i144.i.i = lshr i64 %.63.i43.i134.i.i, 48
  %.6.i4.i24.i145.i.i = shl i64 %.63.i43.i134.i.i, 16
  %.9.i7.i25.i146.i.i = or i64 %.8.i6.i23.i144.i.i, %.6.i4.i24.i145.i.i
  %.63.i26.i147.i.i = xor i64 %.9.i7.i25.i146.i.i, %.11.i19.i140.i.i
  %.8.i.i27.i148.i.i = lshr i64 %.38.i47.i138.i.i, 51
  %.6.i.i28.i149.i.i = shl i64 %.38.i47.i138.i.i, 13
  %.9.i.i29.i150.i.i = or i64 %.8.i.i27.i148.i.i, %.6.i.i28.i149.i.i
  %.38.i30.i151.i.i = xor i64 %.9.i.i29.i150.i.i, %.10.i18.i139.i.i
  %.10.i1.i152.i.i = add i64 %.11.i19.i140.i.i, %.38.i30.i151.i.i
  %.11.i2.i153.i.i = add i64 %.63.i26.i147.i.i, %.9.i3.i22.i143.i.i
  %.8.i2.i3.i154.i.i = lshr i64 %.10.i1.i152.i.i, 32
  %.6.i1.i4.i155.i.i = shl i64 %.10.i1.i152.i.i, 32
  %.9.i3.i5.i156.i.i = or i64 %.8.i2.i3.i154.i.i, %.6.i1.i4.i155.i.i
  %.8.i6.i6.i157.i.i = lshr i64 %.63.i26.i147.i.i, 43
  %.6.i4.i7.i158.i.i = shl i64 %.63.i26.i147.i.i, 21
  %.9.i7.i8.i159.i.i = or i64 %.8.i6.i6.i157.i.i, %.6.i4.i7.i158.i.i
  %.63.i9.i160.i.i = xor i64 %.9.i7.i8.i159.i.i, %.11.i2.i153.i.i
  %.8.i.i10.i161.i.i = lshr i64 %.38.i30.i151.i.i, 47
  %.6.i.i11.i162.i.i = shl i64 %.38.i30.i151.i.i, 17
  %.9.i.i12.i163.i.i = or i64 %.8.i.i10.i161.i.i, %.6.i.i11.i162.i.i
  %.38.i13.i164.i.i = xor i64 %.9.i.i12.i163.i.i, %.10.i1.i152.i.i
  %.557.i.i = xor i64 %.11.i2.i153.i.i, %.499.i.i
  %.559.i.i = xor i64 %.9.i3.i5.i156.i.i, 255
  %.10.i.i57.i.i = add i64 %.557.i.i, %.38.i13.i164.i.i
  %.11.i.i58.i.i = add i64 %.63.i9.i160.i.i, %.559.i.i
  %.8.i2.i.i59.i.i = lshr i64 %.10.i.i57.i.i, 32
  %.6.i1.i.i60.i.i = shl i64 %.10.i.i57.i.i, 32
  %.9.i3.i.i61.i.i = or i64 %.8.i2.i.i59.i.i, %.6.i1.i.i60.i.i
  %.8.i6.i.i62.i.i = lshr i64 %.63.i9.i160.i.i, 48
  %.6.i4.i.i63.i.i = shl i64 %.63.i9.i160.i.i, 16
  %.9.i7.i.i64.i.i = or i64 %.8.i6.i.i62.i.i, %.6.i4.i.i63.i.i
  %.63.i.i65.i.i = xor i64 %.9.i7.i.i64.i.i, %.11.i.i58.i.i
  %.8.i.i.i66.i.i = lshr i64 %.38.i13.i164.i.i, 51
  %.6.i.i.i67.i.i = shl i64 %.38.i13.i164.i.i, 13
  %.9.i.i.i68.i.i = or i64 %.8.i.i.i66.i.i, %.6.i.i.i67.i.i
  %.38.i.i69.i.i = xor i64 %.10.i.i57.i.i, %.9.i.i.i68.i.i
  %.10.i35.i70.i.i = add i64 %.38.i.i69.i.i, %.11.i.i58.i.i
  %.11.i36.i71.i.i = add i64 %.63.i.i65.i.i, %.9.i3.i.i61.i.i
  %.8.i2.i37.i72.i.i = lshr i64 %.10.i35.i70.i.i, 32
  %.6.i1.i38.i73.i.i = shl i64 %.10.i35.i70.i.i, 32
  %.9.i3.i39.i74.i.i = or i64 %.8.i2.i37.i72.i.i, %.6.i1.i38.i73.i.i
  %.8.i6.i40.i75.i.i = lshr i64 %.63.i.i65.i.i, 43
  %.6.i4.i41.i76.i.i = shl i64 %.63.i.i65.i.i, 21
  %.9.i7.i42.i77.i.i = or i64 %.8.i6.i40.i75.i.i, %.6.i4.i41.i76.i.i
  %.63.i43.i78.i.i = xor i64 %.9.i7.i42.i77.i.i, %.11.i36.i71.i.i
  %.8.i.i44.i79.i.i = lshr i64 %.38.i.i69.i.i, 47
  %.6.i.i45.i80.i.i = shl i64 %.38.i.i69.i.i, 17
  %.9.i.i46.i81.i.i = or i64 %.8.i.i44.i79.i.i, %.6.i.i45.i80.i.i
  %.38.i47.i82.i.i = xor i64 %.9.i.i46.i81.i.i, %.10.i35.i70.i.i
  %.10.i18.i83.i.i = add i64 %.38.i47.i82.i.i, %.11.i36.i71.i.i
  %.11.i19.i84.i.i = add i64 %.63.i43.i78.i.i, %.9.i3.i39.i74.i.i
  %.8.i2.i20.i85.i.i = lshr i64 %.10.i18.i83.i.i, 32
  %.6.i1.i21.i86.i.i = shl i64 %.10.i18.i83.i.i, 32
  %.9.i3.i22.i87.i.i = or i64 %.8.i2.i20.i85.i.i, %.6.i1.i21.i86.i.i
  %.8.i6.i23.i88.i.i = lshr i64 %.63.i43.i78.i.i, 48
  %.6.i4.i24.i89.i.i = shl i64 %.63.i43.i78.i.i, 16
  %.9.i7.i25.i90.i.i = or i64 %.8.i6.i23.i88.i.i, %.6.i4.i24.i89.i.i
  %.63.i26.i91.i.i = xor i64 %.9.i7.i25.i90.i.i, %.11.i19.i84.i.i
  %.8.i.i27.i92.i.i = lshr i64 %.38.i47.i82.i.i, 51
  %.6.i.i28.i93.i.i = shl i64 %.38.i47.i82.i.i, 13
  %.9.i.i29.i94.i.i = or i64 %.8.i.i27.i92.i.i, %.6.i.i28.i93.i.i
  %.38.i30.i95.i.i = xor i64 %.9.i.i29.i94.i.i, %.10.i18.i83.i.i
  %.10.i1.i96.i.i = add i64 %.38.i30.i95.i.i, %.11.i19.i84.i.i
  %.11.i2.i97.i.i = add i64 %.63.i26.i91.i.i, %.9.i3.i22.i87.i.i
  %.8.i2.i3.i98.i.i = lshr i64 %.10.i1.i96.i.i, 32
  %.6.i1.i4.i99.i.i = shl i64 %.10.i1.i96.i.i, 32
  %.9.i3.i5.i100.i.i = or i64 %.8.i2.i3.i98.i.i, %.6.i1.i4.i99.i.i
  %.8.i6.i6.i101.i.i = lshr i64 %.63.i26.i91.i.i, 43
  %.6.i4.i7.i102.i.i = shl i64 %.63.i26.i91.i.i, 21
  %.9.i7.i8.i103.i.i = or i64 %.8.i6.i6.i101.i.i, %.6.i4.i7.i102.i.i
  %.63.i9.i104.i.i = xor i64 %.9.i7.i8.i103.i.i, %.11.i2.i97.i.i
  %.8.i.i10.i105.i.i = lshr i64 %.38.i30.i95.i.i, 47
  %.6.i.i11.i106.i.i = shl i64 %.38.i30.i95.i.i, 17
  %.9.i.i12.i107.i.i = or i64 %.8.i.i10.i105.i.i, %.6.i.i11.i106.i.i
  %.38.i13.i108.i.i = xor i64 %.9.i.i12.i107.i.i, %.10.i1.i96.i.i
  %.10.i.i1.i.i = add i64 %.38.i13.i108.i.i, %.11.i2.i97.i.i
  %.11.i.i2.i.i = add i64 %.63.i9.i104.i.i, %.9.i3.i5.i100.i.i
  %.8.i2.i.i3.i.i = lshr i64 %.10.i.i1.i.i, 32
  %.6.i1.i.i4.i.i = shl i64 %.10.i.i1.i.i, 32
  %.9.i3.i.i5.i.i = or i64 %.8.i2.i.i3.i.i, %.6.i1.i.i4.i.i
  %.8.i6.i.i6.i.i = lshr i64 %.63.i9.i104.i.i, 48
  %.6.i4.i.i7.i.i = shl i64 %.63.i9.i104.i.i, 16
  %.9.i7.i.i8.i.i = or i64 %.8.i6.i.i6.i.i, %.6.i4.i.i7.i.i
  %.63.i.i9.i.i = xor i64 %.9.i7.i.i8.i.i, %.11.i.i2.i.i
  %.8.i.i.i10.i.i = lshr i64 %.38.i13.i108.i.i, 51
  %.6.i.i.i11.i.i = shl i64 %.38.i13.i108.i.i, 13
  %.9.i.i.i12.i.i = or i64 %.8.i.i.i10.i.i, %.6.i.i.i11.i.i
  %.38.i.i13.i.i = xor i64 %.9.i.i.i12.i.i, %.10.i.i1.i.i
  %.10.i35.i14.i.i = add i64 %.38.i.i13.i.i, %.11.i.i2.i.i
  %.11.i36.i15.i.i = add i64 %.63.i.i9.i.i, %.9.i3.i.i5.i.i
  %.8.i2.i37.i16.i.i = lshr i64 %.10.i35.i14.i.i, 32
  %.6.i1.i38.i17.i.i = shl i64 %.10.i35.i14.i.i, 32
  %.9.i3.i39.i18.i.i = or i64 %.8.i2.i37.i16.i.i, %.6.i1.i38.i17.i.i
  %.8.i6.i40.i19.i.i = lshr i64 %.63.i.i9.i.i, 43
  %.6.i4.i41.i20.i.i = shl i64 %.63.i.i9.i.i, 21
  %.9.i7.i42.i21.i.i = or i64 %.8.i6.i40.i19.i.i, %.6.i4.i41.i20.i.i
  %.63.i43.i22.i.i = xor i64 %.9.i7.i42.i21.i.i, %.11.i36.i15.i.i
  %.8.i.i44.i23.i.i = lshr i64 %.38.i.i13.i.i, 47
  %.6.i.i45.i24.i.i = shl i64 %.38.i.i13.i.i, 17
  %.9.i.i46.i25.i.i = or i64 %.8.i.i44.i23.i.i, %.6.i.i45.i24.i.i
  %.38.i47.i26.i.i = xor i64 %.9.i.i46.i25.i.i, %.10.i35.i14.i.i
  %.10.i18.i27.i.i = add i64 %.38.i47.i26.i.i, %.11.i36.i15.i.i
  %.11.i19.i28.i.i = add i64 %.63.i43.i22.i.i, %.9.i3.i39.i18.i.i
  %.8.i6.i23.i32.i.i = lshr i64 %.63.i43.i22.i.i, 48
  %.6.i4.i24.i33.i.i = shl i64 %.63.i43.i22.i.i, 16
  %.9.i7.i25.i34.i.i = or i64 %.8.i6.i23.i32.i.i, %.6.i4.i24.i33.i.i
  %.63.i26.i35.i.i = xor i64 %.9.i7.i25.i34.i.i, %.11.i19.i28.i.i
  %.8.i.i27.i36.i.i = lshr i64 %.38.i47.i26.i.i, 51
  %.6.i.i28.i37.i.i = shl i64 %.38.i47.i26.i.i, 13
  %.9.i.i29.i38.i.i = or i64 %.8.i.i27.i36.i.i, %.6.i.i28.i37.i.i
  %.38.i30.i39.i.i = xor i64 %.9.i.i29.i38.i.i, %.10.i18.i27.i.i
  %.10.i1.i40.i.i = add i64 %.38.i30.i39.i.i, %.11.i19.i28.i.i
  %.8.i2.i3.i42.i.i = lshr i64 %.10.i1.i40.i.i, 32
  %.6.i1.i4.i43.i.i = shl i64 %.10.i1.i40.i.i, 32
  %.9.i3.i5.i44.i.i = or i64 %.8.i2.i3.i42.i.i, %.6.i1.i4.i43.i.i
  %.8.i6.i6.i45.i.i = lshr i64 %.63.i26.i35.i.i, 43
  %.6.i4.i7.i46.i.i = shl i64 %.63.i26.i35.i.i, 21
  %.9.i7.i8.i47.i.i = or i64 %.8.i6.i6.i45.i.i, %.6.i4.i7.i46.i.i
  %.8.i.i10.i49.i.i = lshr i64 %.38.i30.i39.i.i, 47
  %.6.i.i11.i50.i.i = shl i64 %.38.i30.i39.i.i, 17
  %.9.i.i12.i51.i.i = or i64 %.8.i.i10.i49.i.i, %.6.i.i11.i50.i.i
  %.38.i13.i52.i.i = xor i64 %.9.i.i12.i51.i.i, %.10.i1.i40.i.i
  %.642.i.i = xor i64 %.38.i13.i52.i.i, %.9.i3.i5.i44.i.i
  %.644.i.i = xor i64 %.642.i.i, %.9.i7.i8.i47.i.i
  %.8.i1.i.i.i.i = icmp eq i64 %.644.i.i, -1
  %spec.select.i2.i.i.i.i = select i1 %.8.i1.i.i.i.i, i64 -2, i64 %.644.i.i
  %.225.i = icmp ult i64 %spec.select.i2.i.i.i.i, -2
  %spec.select.i = select i1 %.225.i, i64 %spec.select.i2.i.i.i.i, i64 -43
  %.227.i = getelementptr inbounds i8, i8* %.198.i, i64 16
  %7 = bitcast i8* %.227.i to i64*
  %.228.i = load i64, i64* %7, align 8, !noalias !17
  %.232.i = and i64 %spec.select.i, %.228.i
  %.244.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %.232.i, i32 0
  %.245.i = load i64, i64* %.244.i, align 8, !noalias !17
  %.246.i = icmp eq i64 %spec.select.i, %.245.i
  br i1 %.246.i, label %for.body.if.i, label %for.body.endif.i

lookup.body.i:                                    ; preds = %for.body.endif.endif.endif.2.i, %lookup.body.endif.endif.endif.i
  %.229.0.i = phi i64 [ %.371.i, %lookup.body.endif.endif.endif.i ], [ %spec.select.i, %for.body.endif.endif.endif.2.i ]
  %.374.pn.i = phi i64 [ %.374.i, %lookup.body.endif.endif.endif.i ], [ %.303.2.i, %for.body.endif.endif.endif.2.i ]
  %.236.0.i = phi i64 [ %.236.4.i, %lookup.body.endif.endif.endif.i ], [ %.236.3.2.i, %for.body.endif.endif.endif.2.i ]
  %.233.0.i = and i64 %.374.pn.i, %.228.i
  %.311.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %.233.0.i, i32 0
  %.312.i = load i64, i64* %.311.i, align 8, !noalias !17
  %.313.i = icmp eq i64 %spec.select.i, %.312.i
  br i1 %.313.i, label %lookup.body.if.i, label %lookup.body.endif.i

for.body.if.i:                                    ; preds = %B104.i.i.i.i
  %.249.elt.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %.232.i, i32 1, i32 0
  %.249.unpack.i = load i8*, i8** %.249.elt.i, align 8, !noalias !25
  %.249.elt167.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %.232.i, i32 1, i32 1
  %.249.unpack168.i = load i64, i64* %.249.elt167.i, align 8, !noalias !17
  %.249.elt169.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %.232.i, i32 1, i32 2
  %.249.unpack170.i = load i32, i32* %.249.elt169.i, align 8, !noalias !25
  %.192.not.i5.i = icmp eq i64 %.249.unpack168.i, 1
  br i1 %.192.not.i5.i, label %B78.endif.i8.i, label %for.body.endif.i

B78.endif.i8.i:                                   ; preds = %for.body.if.i
  %.93.i12.i.i = bitcast i8* %.249.unpack.i to i32*
  %.64.i7.i.i = bitcast i8* %.249.unpack.i to i16*
  %8 = icmp eq i32 %.249.unpack170.i, 1
  br i1 %8, label %B60.us.us.i.i.critedge, label %B58.us.preheader.i.i

B58.us.preheader.i.i:                             ; preds = %B78.endif.i8.i
  %.23.i2.i.i = sext i32 %.249.unpack170.i to i64
  switch i64 %.23.i2.i.i, label %B60.us.i.i.critedge [
    i64 4, label %B60.us.us83.i.i.critedge
    i64 2, label %B60.us.us96.i.i.critedge
  ]

B60.us.us83.i.i.critedge:                         ; preds = %B58.us.preheader.i.i
  %.95.i14.us.us.i.i = load i32, i32* %.93.i12.i.i, align 4, !noalias !58
  %9 = zext i8 %.483.i.i to i32
  %.not71 = icmp eq i32 %.95.i14.us.us.i.i, %9
  br i1 %.not71, label %lookup.end.endif.i, label %for.body.endif.i

B60.us.us96.i.i.critedge:                         ; preds = %B58.us.preheader.i.i
  %.66.i9.us.us105.i.i = load i16, i16* %.64.i7.i.i, align 2, !noalias !58
  %10 = zext i8 %.483.i.i to i16
  %.not70 = icmp eq i16 %.66.i9.us.us105.i.i, %10
  br i1 %.not70, label %lookup.end.endif.i, label %for.body.endif.i

B60.us.us.i.i.critedge:                           ; preds = %B78.endif.i8.i
  %.37.i4.us.us.i.i = load i8, i8* %.249.unpack.i, align 1, !noalias !58
  %.not72 = icmp eq i8 %.483.i.i, %.37.i4.us.us.i.i
  br i1 %.not72, label %lookup.end.endif.i, label %for.body.endif.i

B60.us.i.i.critedge:                              ; preds = %B58.us.preheader.i.i
  %.298.us.not.i.i = icmp eq i8 %.483.i.i, 0
  br i1 %.298.us.not.i.i, label %lookup.end.endif.i, label %for.body.endif.i

for.body.endif.i:                                 ; preds = %B60.us.us.i.i.critedge, %B60.us.us83.i.i.critedge, %B60.us.us96.i.i.critedge, %B60.us.i.i.critedge, %for.body.if.i, %B104.i.i.i.i
  switch i64 %.245.i, label %for.body.endif.endif.endif.i [
    i64 -1, label %lookup.end.if.thread.i
    i64 -2, label %for.body.endif.endif.if.i
  ]

for.body.endif.endif.if.i:                        ; preds = %for.body.endif.i
  br label %for.body.endif.endif.endif.i

for.body.endif.endif.endif.i:                     ; preds = %for.body.endif.endif.if.i, %for.body.endif.i
  %.236.3.i = phi i64 [ -1, %for.body.endif.i ], [ %.232.i, %for.body.endif.endif.if.i ]
  %.303.i = add i64 %.232.i, 1
  %.304.i = and i64 %.303.i, %.228.i
  %.244.1.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %.304.i, i32 0
  %.245.1.i = load i64, i64* %.244.1.i, align 8, !noalias !17
  %.246.1.i = icmp eq i64 %spec.select.i, %.245.1.i
  br i1 %.246.1.i, label %for.body.if.1.i, label %for.body.endif.1.i

lookup.body.if.i:                                 ; preds = %lookup.body.i
  %.316.elt.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %.233.0.i, i32 1, i32 0
  %.316.unpack.i = load i8*, i8** %.316.elt.i, align 8, !noalias !63
  %.316.elt153.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %.233.0.i, i32 1, i32 1
  %.316.unpack154.i = load i64, i64* %.316.elt153.i, align 8, !noalias !17
  %.316.elt155.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %.233.0.i, i32 1, i32 2
  %.316.unpack156.i = load i32, i32* %.316.elt155.i, align 8, !noalias !63
  %.192.not.i18.i = icmp eq i64 %.316.unpack154.i, 1
  br i1 %.192.not.i18.i, label %B78.endif.i21.i, label %lookup.body.endif.i

B78.endif.i21.i:                                  ; preds = %lookup.body.if.i
  %.93.i12.i458.i = bitcast i8* %.316.unpack.i to i32*
  %.64.i7.i459.i = bitcast i8* %.316.unpack.i to i16*
  %11 = icmp eq i32 %.316.unpack156.i, 1
  br i1 %11, label %B60.us.us.i497.i.critedge, label %B58.us.preheader.i460.i

B58.us.preheader.i460.i:                          ; preds = %B78.endif.i21.i
  %.23.i2.i457.i = sext i32 %.316.unpack156.i to i64
  switch i64 %.23.i2.i457.i, label %lookup.body.endif.i [
    i64 4, label %B60.us.us83.i471.i.critedge
    i64 2, label %B60.us.us96.i485.i.critedge
  ]

B60.us.us83.i471.i.critedge:                      ; preds = %B58.us.preheader.i460.i
  %.95.i14.us.us.i468.i = load i32, i32* %.93.i12.i458.i, align 4, !noalias !67
  %.not62 = icmp eq i32 %.95.i14.us.us.i468.i, %36
  br i1 %.not62, label %lookup.end.endif.i, label %lookup.body.endif.i

B60.us.us96.i485.i.critedge:                      ; preds = %B58.us.preheader.i460.i
  %.66.i9.us.us105.i482.i = load i16, i16* %.64.i7.i459.i, align 2, !noalias !67
  %.not = icmp eq i16 %.66.i9.us.us105.i482.i, %35
  br i1 %.not, label %lookup.end.endif.i, label %lookup.body.endif.i

B60.us.us.i497.i.critedge:                        ; preds = %B78.endif.i21.i
  %.37.i4.us.us.i495.i = load i8, i8* %.316.unpack.i, align 1, !noalias !67
  %.not63 = icmp eq i8 %.483.i.i, %.37.i4.us.us.i495.i
  br i1 %.not63, label %lookup.end.endif.i, label %lookup.body.endif.i

lookup.body.endif.i:                              ; preds = %B58.us.preheader.i460.i, %B60.us.us.i497.i.critedge, %B60.us.us83.i471.i.critedge, %B60.us.us96.i485.i.critedge, %lookup.body.if.i, %lookup.body.i
  switch i64 %.312.i, label %lookup.body.endif.endif.endif.i [
    i64 -1, label %lookup.end.if.i
    i64 -2, label %lookup.body.endif.endif.if.i
  ]

lookup.body.endif.endif.if.i:                     ; preds = %lookup.body.endif.i
  %.366.i = icmp eq i64 %.236.0.i, -1
  %.367.i = select i1 %.366.i, i64 %.233.0.i, i64 %.236.0.i
  br label %lookup.body.endif.endif.endif.i

lookup.body.endif.endif.endif.i:                  ; preds = %lookup.body.endif.endif.if.i, %lookup.body.endif.i
  %.236.4.i = phi i64 [ %.236.0.i, %lookup.body.endif.i ], [ %.367.i, %lookup.body.endif.endif.if.i ]
  %.371.i = lshr i64 %.229.0.i, 5
  %.372.i = mul i64 %.233.0.i, 5
  %.373.i = add nuw nsw i64 %.371.i, 1
  %.374.i = add i64 %.373.i, %.372.i
  br label %lookup.body.i

lookup.end.if.i:                                  ; preds = %lookup.body.endif.i, %lookup.body.endif.i.us, %for.body.endif.2.i, %for.body.endif.1.i
  %.233.1.i = phi i64 [ %.304.i, %for.body.endif.1.i ], [ %.304.1.i, %for.body.endif.2.i ], [ %.233.0.i.us, %lookup.body.endif.i.us ], [ %.233.0.i, %lookup.body.endif.i ]
  %.236.1.i = phi i64 [ %.236.3.i, %for.body.endif.1.i ], [ %.236.3.1.i, %for.body.endif.2.i ], [ %.236.0.i.us, %lookup.body.endif.i.us ], [ %.236.0.i, %lookup.body.endif.i ]
  %.381.i = icmp eq i64 %.236.1.i, -1
  br i1 %.381.i, label %lookup.end.if.thread.i, label %12

lookup.end.if.thread.i:                           ; preds = %lookup.end.if.i, %for.body.endif.i
  %.233.1385.i = phi i64 [ %.233.1.i, %lookup.end.if.i ], [ %.232.i, %for.body.endif.i ]
  br label %12

12:                                               ; preds = %lookup.end.if.thread.i, %lookup.end.if.i
  %13 = phi i64 [ %.233.1385.i, %lookup.end.if.thread.i ], [ %.236.1.i, %lookup.end.if.i ]
  %.390.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %13, i32 0
  %.391.i = load i64, i64* %.390.i, align 8, !noalias !17
  store i64 %spec.select.i, i64* %.390.i, align 8, !noalias !17
  %.395.repack.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %13, i32 1, i32 0
  store i8* %.6.i.i.i.i, i8** %.395.repack.i, align 8, !noalias !17
  %.395.repack88.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %13, i32 1, i32 1
  store i64 1, i64* %.395.repack88.i, align 8, !noalias !17
  %.395.repack90.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %13, i32 1, i32 2
  store i32 1, i32* %.395.repack90.i, align 8, !noalias !17
  %.395.repack92.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %13, i32 1, i32 3
  store i32 1, i32* %.395.repack92.i, align 4, !noalias !17
  %.395.repack94.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %13, i32 1, i32 4
  store i64 -1, i64* %.395.repack94.i, align 8, !noalias !17
  %.395.repack96.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %13, i32 1, i32 5
  %14 = bitcast i8** %.395.repack96.i to %Struct.MemInfo.11**
  store %Struct.MemInfo.11* %.49.i.i.i, %Struct.MemInfo.11** %14, align 8, !noalias !17
  %.395.repack98.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %13, i32 1, i32 6
  store i8* null, i8** %.395.repack98.i, align 8, !noalias !17
  %.404.i = getelementptr inbounds i8, i8* %.198.i, i64 8
  %15 = bitcast i8* %.404.i to i64*
  %.405.i = load i64, i64* %15, align 8, !noalias !17
  %.406.i = add i64 %.405.i, 1
  store i64 %.406.i, i64* %15, align 8, !noalias !17
  %.409.i = icmp eq i64 %.391.i, -1
  br i1 %.409.i, label %lookup.end.if.if.i, label %lookup.end.if.endif.i, !prof !72

lookup.end.endif.i:                               ; preds = %B60.us.us.i497.i.critedge, %B60.us.us83.i471.i.critedge, %B60.us.us96.i485.i.critedge, %B60.us.us96.i485.i.critedge.us, %B60.us.us83.i471.i.critedge.us, %B60.us.us.i497.i.critedge.us, %B58.us.preheader.i460.i.us, %B60.us.us.i.i.critedge, %B60.us.us83.i.i.critedge, %B60.us.us96.i.i.critedge, %B60.us.us.i615.i.critedge, %B60.us.us83.i589.i.critedge, %B60.us.us96.i603.i.critedge, %B60.us.us.i556.i.critedge, %B60.us.us83.i530.i.critedge, %B60.us.us96.i544.i.critedge, %B60.us.i567.i.critedge, %B60.us.i626.i.critedge, %B60.us.i.i.critedge, %for.end.1.i, %lookup.end.if.endif.i
  br i1 %.131.i, label %for_iter.body.endif.if.i, label %_ZN3rbc5tests7heavydb8test_nrt15test_set_simple12_3clocals_3e2fnB2v1B44c8tJTC_2fWQA9wW1DkAz0Pj1skAdT4gkkUlYBZmgA_3dE73TextEncodingNonePointer_5bSTRUCT__cPW3WptrLW2WszV5Vbool8W7Wis_nullK_5d_2a.exit

lookup.end.if.if.i:                               ; preds = %12
  %.411.i = bitcast i8* %.198.i to i64*
  %.412.i = load i64, i64* %.411.i, align 8, !noalias !17
  %.413.i = add i64 %.412.i, 1
  store i64 %.413.i, i64* %.411.i, align 8, !noalias !17
  br label %lookup.end.if.endif.i

lookup.end.if.endif.i:                            ; preds = %lookup.end.if.if.i, %12
  %.419.i = tail call fastcc i8* @NRT_MemInfo_data_fast(i8* nonnull %0) #7, !noalias !17
  %.421.i = getelementptr inbounds i8, i8* %.419.i, i64 40
  %16 = bitcast i8* %.421.i to { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }*
  %.422.i = shl i64 %.406.i, 1
  %.423.i = getelementptr inbounds i8, i8* %.419.i, i64 16
  %17 = bitcast i8* %.423.i to i64*
  %.424.i = load i64, i64* %17, align 8, !noalias !17
  %.425.i = add i64 %.424.i, 1
  %.426.not.i = icmp ult i64 %.422.i, %.425.i
  br i1 %.426.not.i, label %lookup.end.endif.i, label %calcsize.body.i, !prof !72

calcsize.body.i:                                  ; preds = %lookup.end.if.endif.i, %calcsize.body.i
  %.428.0.i = phi i64 [ %.433.i, %calcsize.body.i ], [ %.425.i, %lookup.end.if.endif.i ]
  %.433.i = shl i64 %.428.0.i, 2
  %.435.not.i = icmp ult i64 %.422.i, %.433.i
  br i1 %.435.not.i, label %calcsize.end.i, label %calcsize.body.i

calcsize.end.i:                                   ; preds = %calcsize.body.i
  %.441.i = tail call { i64, i1 } @llvm.smul.with.overflow.i64(i64 %.433.i, i64 56) #7
  %.442.i = extractvalue { i64, i1 } %.441.i, 0
  %.443.i = extractvalue { i64, i1 } %.441.i, 1
  %.444.i = tail call { i64, i1 } @llvm.sadd.with.overflow.i64(i64 %.442.i, i64 40) #7
  %.445.i = extractvalue { i64, i1 } %.444.i, 0
  %.446.i = extractvalue { i64, i1 } %.444.i, 1
  %.447.i = or i1 %.443.i, %.446.i
  br i1 %.447.i, label %entry.if, label %calcsize.end.endif.if.i, !prof !16

calcsize.end.endif.if.i:                          ; preds = %calcsize.end.i
  %.455.i = tail call fastcc i8* @NRT_MemInfo_varsize_alloc(%Struct.MemInfo.11* nonnull %.46.i, i64 %.445.i) #7, !noalias !17
  %.456.i = icmp eq i8* %.455.i, null
  br i1 %.456.i, label %entry.if, label %calcsize.end.endif.endif.endif.i, !prof !16

calcsize.end.endif.endif.endif.i:                 ; preds = %calcsize.end.endif.if.i
  %.462.i = tail call fastcc i8* @NRT_MemInfo_data_fast(i8* nonnull %0) #7, !noalias !17
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 1 dereferenceable(1) %.462.i, i8 -1, i64 %.445.i, i1 false) #7, !noalias !17
  %.470.i = getelementptr inbounds i8, i8* %.462.i, i64 24
  %18 = bitcast i8* %.470.i to i64*
  store i64 0, i64* %18, align 8, !noalias !17
  %.472.i = add nsw i64 %.433.i, -1
  %.473.i = getelementptr inbounds i8, i8* %.462.i, i64 16
  %19 = bitcast i8* %.473.i to i64*
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %.462.i, i8 0, i64 16, i1 false) #7, !noalias !17
  store i64 %.472.i, i64* %19, align 8, !noalias !17
  %.487.i = tail call fastcc i8* @NRT_MemInfo_data_fast(i8* nonnull %0) #7, !noalias !17
  %.489.i = getelementptr inbounds i8, i8* %.487.i, i64 40
  %20 = bitcast i8* %.489.i to { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }*
  %.491.i = load i64, i64* %17, align 8, !noalias !17
  %.492.i = add i64 %.491.i, 1
  %.49480.i = icmp sgt i64 %.492.i, 0
  br i1 %.49480.i, label %for.body.1.lr.ph.i, label %for.end.1.i

for.body.1.lr.ph.i:                               ; preds = %calcsize.end.endif.endif.endif.i
  %.512.i = getelementptr inbounds i8, i8* %.487.i, i64 16
  %21 = bitcast i8* %.512.i to i64*
  %.688.i = getelementptr inbounds i8, i8* %.487.i, i64 8
  %22 = bitcast i8* %.688.i to i64*
  %.695.i = bitcast i8* %.487.i to i64*
  br label %for.body.1.i

for.body.1.i:                                     ; preds = %for.body.1.endif.i, %for.body.1.lr.ph.i
  %loop.index.181.i = phi i64 [ 0, %for.body.1.lr.ph.i ], [ %.703.i, %for.body.1.endif.i ]
  %.497.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %16, i64 %loop.index.181.i, i32 0
  %.498.i = load i64, i64* %.497.i, align 8, !noalias !17
  %.499.i = icmp ult i64 %.498.i, -2
  br i1 %.499.i, label %for.body.1.if.i, label %for.body.1.endif.i

for.end.1.i:                                      ; preds = %for.body.1.endif.i, %calcsize.end.endif.endif.endif.i
  tail call fastcc void @NRT_MemInfo_varsize_free(%Struct.MemInfo.11* nonnull %.46.i, i8* %.419.i) #7, !noalias !17
  br label %lookup.end.endif.i

for.body.1.if.i:                                  ; preds = %for.body.1.i
  %.502.elt.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %16, i64 %loop.index.181.i, i32 1, i32 0
  %.502.unpack.i = load i8*, i8** %.502.elt.i, align 8, !noalias !17
  %.502.elt100.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %16, i64 %loop.index.181.i, i32 1, i32 1
  %.502.unpack101.i = load i64, i64* %.502.elt100.i, align 8, !noalias !17
  %.502.elt102.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %16, i64 %loop.index.181.i, i32 1, i32 2
  %.502.unpack103.i = load i32, i32* %.502.elt102.i, align 8, !noalias !17
  %.502.elt104.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %16, i64 %loop.index.181.i, i32 1, i32 3
  %.502.unpack105.i = load i32, i32* %.502.elt104.i, align 4, !noalias !17
  %.502.elt106.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %16, i64 %loop.index.181.i, i32 1, i32 4
  %.502.unpack107.i = load i64, i64* %.502.elt106.i, align 8, !noalias !17
  %.502.elt108.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %16, i64 %loop.index.181.i, i32 1, i32 5
  %23 = bitcast i8** %.502.elt108.i to i64*
  %.502.unpack109.i41 = load i64, i64* %23, align 8, !noalias !17
  %.502.elt110.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %16, i64 %loop.index.181.i, i32 1, i32 6
  %24 = bitcast i8** %.502.elt110.i to i64*
  %.502.unpack111.i42 = load i64, i64* %24, align 8, !noalias !17
  %.513.i = load i64, i64* %21, align 8, !noalias !17
  %.517.i = and i64 %.513.i, %.498.i
  %.529.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %.517.i, i32 0
  %.530.i = load i64, i64* %.529.i, align 8, !noalias !17
  %.531.i = icmp eq i64 %.498.i, %.530.i
  br i1 %.531.i, label %for.body.2.if.i, label %for.body.2.endif.i

for.body.1.endif.i:                               ; preds = %B78.endif.i.i, %B78.endif.i34.2.i, %B78.endif.i34.1.i, %lookup.end.1.if.if.i, %25, %B78.endif.i34.i, %for.body.1.i
  %.703.i = add nuw nsw i64 %loop.index.181.i, 1
  %exitcond.not.i = icmp eq i64 %loop.index.181.i, %.491.i
  br i1 %exitcond.not.i, label %for.end.1.i, label %for.body.1.i

lookup.body.1.i:                                  ; preds = %for.body.2.endif.endif.endif.2.i, %lookup.body.1.endif.endif.endif.i
  %.514.0.i = phi i64 [ %.656.i, %lookup.body.1.endif.endif.endif.i ], [ %.498.i, %for.body.2.endif.endif.endif.2.i ]
  %.659.pn.i = phi i64 [ %.659.i, %lookup.body.1.endif.endif.endif.i ], [ %.588.2.i, %for.body.2.endif.endif.endif.2.i ]
  %.521.0.i = phi i64 [ %.521.4.i, %lookup.body.1.endif.endif.endif.i ], [ %.521.3.2.i, %for.body.2.endif.endif.endif.2.i ]
  %.518.0.i = and i64 %.659.pn.i, %.513.i
  %.596.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %.518.0.i, i32 0
  %.597.i = load i64, i64* %.596.i, align 8, !noalias !17
  %.598.i = icmp eq i64 %.498.i, %.597.i
  br i1 %.598.i, label %lookup.body.1.if.i, label %lookup.body.1.endif.i

for.body.2.if.i:                                  ; preds = %for.body.1.if.i
  %.534.elt.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %.517.i, i32 1, i32 0
  %.534.unpack.i = load i8*, i8** %.534.elt.i, align 8, !noalias !17
  %.534.elt139.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %.517.i, i32 1, i32 1
  %.534.unpack140.i = load i64, i64* %.534.elt139.i, align 8, !noalias !17
  %.534.elt141.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %.517.i, i32 1, i32 2
  %.534.unpack142.i = load i32, i32* %.534.elt141.i, align 8, !noalias !17
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %4) #7, !noalias !17
  store i64 0, i64* %.230.i29.i, align 8, !noalias !73
  %.192.not.i31.i = icmp eq i64 %.502.unpack101.i, %.534.unpack140.i
  br i1 %.192.not.i31.i, label %B78.endif.i34.i, label %for.body.2.if.endif.endif.thread283.i

for.body.2.if.endif.endif.thread283.i:            ; preds = %for.body.2.if.i
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %4) #7, !noalias !17
  br label %for.body.2.endif.i

B78.endif.i34.i:                                  ; preds = %for.body.2.if.i
  store i64 0, i64* %.230.i29.i, align 8, !noalias !73
  call fastcc void @_ZN5numba7cpython7unicode11_cmp_regionB3v35B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex12unicode_typexx(i64* nonnull %.230.i29.i, i8* %.502.unpack.i, i64 %.502.unpack101.i, i32 %.502.unpack103.i, i8* %.534.unpack.i, i64 %.502.unpack101.i, i32 %.534.unpack142.i, i64 %.502.unpack101.i) #7
  %.244.i36.i = load i64, i64* %.230.i29.i, align 8, !noalias !73
  %.260.i37.not.i = icmp eq i64 %.244.i36.i, 0
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %4) #7, !noalias !17
  br i1 %.260.i37.not.i, label %for.body.1.endif.i, label %for.body.2.endif.i

for.body.2.endif.i:                               ; preds = %B78.endif.i34.i, %for.body.2.if.endif.endif.thread283.i, %for.body.1.if.i
  switch i64 %.530.i, label %for.body.2.endif.endif.endif.i [
    i64 -1, label %lookup.end.1.if.thread.i
    i64 -2, label %for.body.2.endif.endif.if.i
  ]

for.body.2.endif.endif.if.i:                      ; preds = %for.body.2.endif.i
  br label %for.body.2.endif.endif.endif.i

for.body.2.endif.endif.endif.i:                   ; preds = %for.body.2.endif.endif.if.i, %for.body.2.endif.i
  %.521.3.i = phi i64 [ -1, %for.body.2.endif.i ], [ %.517.i, %for.body.2.endif.endif.if.i ]
  %.588.i = add nuw i64 %.517.i, 1
  %.589.i = and i64 %.588.i, %.513.i
  %.529.1.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %.589.i, i32 0
  %.530.1.i = load i64, i64* %.529.1.i, align 8, !noalias !17
  %.531.1.i = icmp eq i64 %.498.i, %.530.1.i
  br i1 %.531.1.i, label %for.body.2.if.1.i, label %for.body.2.endif.1.i

lookup.body.1.if.i:                               ; preds = %lookup.body.1.i
  %.601.elt.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %.518.0.i, i32 1, i32 0
  %.601.unpack.i = load i8*, i8** %.601.elt.i, align 8, !noalias !17
  %.601.elt125.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %.518.0.i, i32 1, i32 1
  %.601.unpack126.i = load i64, i64* %.601.elt125.i, align 8, !noalias !17
  %.601.elt127.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %.518.0.i, i32 1, i32 2
  %.601.unpack128.i = load i32, i32* %.601.elt127.i, align 8, !noalias !17
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %5) #7, !noalias !17
  store i64 0, i64* %.230.i.i, align 8, !noalias !77
  %.192.not.i.i = icmp eq i64 %.502.unpack101.i, %.601.unpack126.i
  br i1 %.192.not.i.i, label %B78.endif.i.i, label %lookup.body.1.if.endif.endif.thread294.i

lookup.body.1.if.endif.endif.thread294.i:         ; preds = %lookup.body.1.if.i
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %5) #7, !noalias !17
  br label %lookup.body.1.endif.i

B78.endif.i.i:                                    ; preds = %lookup.body.1.if.i
  store i64 0, i64* %.230.i.i, align 8, !noalias !77
  call fastcc void @_ZN5numba7cpython7unicode11_cmp_regionB3v35B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex12unicode_typexx(i64* nonnull %.230.i.i, i8* %.502.unpack.i, i64 %.502.unpack101.i, i32 %.502.unpack103.i, i8* %.601.unpack.i, i64 %.502.unpack101.i, i32 %.601.unpack128.i, i64 %.502.unpack101.i) #7
  %.244.i.i = load i64, i64* %.230.i.i, align 8, !noalias !77
  %.260.i.not.i = icmp eq i64 %.244.i.i, 0
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %5) #7, !noalias !17
  br i1 %.260.i.not.i, label %for.body.1.endif.i, label %lookup.body.1.endif.i

lookup.body.1.endif.i:                            ; preds = %B78.endif.i.i, %lookup.body.1.if.endif.endif.thread294.i, %lookup.body.1.i
  switch i64 %.597.i, label %lookup.body.1.endif.endif.endif.i [
    i64 -1, label %lookup.end.1.if.i
    i64 -2, label %lookup.body.1.endif.endif.if.i
  ]

lookup.body.1.endif.endif.if.i:                   ; preds = %lookup.body.1.endif.i
  %.651.i = icmp eq i64 %.521.0.i, -1
  %.652.i = select i1 %.651.i, i64 %.518.0.i, i64 %.521.0.i
  br label %lookup.body.1.endif.endif.endif.i

lookup.body.1.endif.endif.endif.i:                ; preds = %lookup.body.1.endif.endif.if.i, %lookup.body.1.endif.i
  %.521.4.i = phi i64 [ %.521.0.i, %lookup.body.1.endif.i ], [ %.652.i, %lookup.body.1.endif.endif.if.i ]
  %.656.i = lshr i64 %.514.0.i, 5
  %.657.i = mul i64 %.518.0.i, 5
  %.658.i = add nuw nsw i64 %.656.i, 1
  %.659.i = add i64 %.658.i, %.657.i
  br label %lookup.body.1.i

lookup.end.1.if.i:                                ; preds = %lookup.body.1.endif.i, %for.body.2.endif.2.i, %for.body.2.endif.1.i
  %.518.1.i = phi i64 [ %.589.i, %for.body.2.endif.1.i ], [ %.589.1.i, %for.body.2.endif.2.i ], [ %.518.0.i, %lookup.body.1.endif.i ]
  %.521.1.i = phi i64 [ %.521.3.i, %for.body.2.endif.1.i ], [ %.521.3.1.i, %for.body.2.endif.2.i ], [ %.521.0.i, %lookup.body.1.endif.i ]
  %.666.i = icmp eq i64 %.521.1.i, -1
  br i1 %.666.i, label %lookup.end.1.if.thread.i, label %25

lookup.end.1.if.thread.i:                         ; preds = %lookup.end.1.if.i, %for.body.2.endif.i
  %.518.1389.i = phi i64 [ %.518.1.i, %lookup.end.1.if.i ], [ %.517.i, %for.body.2.endif.i ]
  br label %25

25:                                               ; preds = %lookup.end.1.if.thread.i, %lookup.end.1.if.i
  %26 = phi i64 [ %.518.1389.i, %lookup.end.1.if.thread.i ], [ %.521.1.i, %lookup.end.1.if.i ]
  %.675.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %26, i32 0
  %.676.i = load i64, i64* %.675.i, align 8, !noalias !17
  store i64 %.498.i, i64* %.675.i, align 8, !noalias !17
  %.679.repack.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %26, i32 1, i32 0
  store i8* %.502.unpack.i, i8** %.679.repack.i, align 8, !noalias !17
  %.679.repack113.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %26, i32 1, i32 1
  store i64 %.502.unpack101.i, i64* %.679.repack113.i, align 8, !noalias !17
  %.679.repack115.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %26, i32 1, i32 2
  store i32 %.502.unpack103.i, i32* %.679.repack115.i, align 8, !noalias !17
  %.679.repack117.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %26, i32 1, i32 3
  store i32 %.502.unpack105.i, i32* %.679.repack117.i, align 4, !noalias !17
  %.679.repack119.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %26, i32 1, i32 4
  store i64 %.502.unpack107.i, i64* %.679.repack119.i, align 8, !noalias !17
  %.679.repack121.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %26, i32 1, i32 5
  %27 = bitcast i8** %.679.repack121.i to i64*
  store i64 %.502.unpack109.i41, i64* %27, align 8, !noalias !17
  %.679.repack123.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %26, i32 1, i32 6
  %28 = bitcast i8** %.679.repack123.i to i64*
  store i64 %.502.unpack111.i42, i64* %28, align 8, !noalias !17
  %.689.i = load i64, i64* %22, align 8, !noalias !17
  %.690.i = add i64 %.689.i, 1
  store i64 %.690.i, i64* %22, align 8, !noalias !17
  %.693.i = icmp eq i64 %.676.i, -1
  br i1 %.693.i, label %lookup.end.1.if.if.i, label %for.body.1.endif.i, !prof !72

lookup.end.1.if.if.i:                             ; preds = %25
  %.696.i = load i64, i64* %.695.i, align 8, !noalias !17
  %.697.i = add i64 %.696.i, 1
  store i64 %.697.i, i64* %.695.i, align 8, !noalias !17
  br label %for.body.1.endif.i

for.body.if.1.i:                                  ; preds = %for.body.endif.endif.endif.i
  %.249.elt.1.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %.304.i, i32 1, i32 0
  %.249.unpack.1.i = load i8*, i8** %.249.elt.1.i, align 8, !noalias !25
  %.249.elt167.1.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %.304.i, i32 1, i32 1
  %.249.unpack168.1.i = load i64, i64* %.249.elt167.1.i, align 8, !noalias !17
  %.249.elt169.1.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %.304.i, i32 1, i32 2
  %.249.unpack170.1.i = load i32, i32* %.249.elt169.1.i, align 8, !noalias !25
  %.192.not.i5.1.i = icmp eq i64 %.249.unpack168.1.i, 1
  br i1 %.192.not.i5.1.i, label %B78.endif.i8.1.i, label %for.body.endif.1.i

B78.endif.i8.1.i:                                 ; preds = %for.body.if.1.i
  %.93.i12.i576.i = bitcast i8* %.249.unpack.1.i to i32*
  %.64.i7.i577.i = bitcast i8* %.249.unpack.1.i to i16*
  %29 = icmp eq i32 %.249.unpack170.1.i, 1
  br i1 %29, label %B60.us.us.i615.i.critedge, label %B58.us.preheader.i578.i

B58.us.preheader.i578.i:                          ; preds = %B78.endif.i8.1.i
  %.23.i2.i575.i = sext i32 %.249.unpack170.1.i to i64
  switch i64 %.23.i2.i575.i, label %B60.us.i626.i.critedge [
    i64 4, label %B60.us.us83.i589.i.critedge
    i64 2, label %B60.us.us96.i603.i.critedge
  ]

B60.us.us83.i589.i.critedge:                      ; preds = %B58.us.preheader.i578.i
  %.95.i14.us.us.i586.i = load i32, i32* %.93.i12.i576.i, align 4, !noalias !81
  %30 = zext i8 %.483.i.i to i32
  %.not68 = icmp eq i32 %.95.i14.us.us.i586.i, %30
  br i1 %.not68, label %lookup.end.endif.i, label %for.body.endif.1.i

B60.us.us96.i603.i.critedge:                      ; preds = %B58.us.preheader.i578.i
  %.66.i9.us.us105.i600.i = load i16, i16* %.64.i7.i577.i, align 2, !noalias !81
  %31 = zext i8 %.483.i.i to i16
  %.not67 = icmp eq i16 %.66.i9.us.us105.i600.i, %31
  br i1 %.not67, label %lookup.end.endif.i, label %for.body.endif.1.i

B60.us.us.i615.i.critedge:                        ; preds = %B78.endif.i8.1.i
  %.37.i4.us.us.i613.i = load i8, i8* %.249.unpack.1.i, align 1, !noalias !81
  %.not69 = icmp eq i8 %.483.i.i, %.37.i4.us.us.i613.i
  br i1 %.not69, label %lookup.end.endif.i, label %for.body.endif.1.i

B60.us.i626.i.critedge:                           ; preds = %B58.us.preheader.i578.i
  %.298.us.not.i625.i = icmp eq i8 %.483.i.i, 0
  br i1 %.298.us.not.i625.i, label %lookup.end.endif.i, label %for.body.endif.1.i

for.body.endif.1.i:                               ; preds = %B60.us.us.i615.i.critedge, %B60.us.us83.i589.i.critedge, %B60.us.us96.i603.i.critedge, %B60.us.i626.i.critedge, %for.body.if.1.i, %for.body.endif.endif.endif.i
  switch i64 %.245.1.i, label %for.body.endif.endif.endif.1.i [
    i64 -1, label %lookup.end.if.i
    i64 -2, label %for.body.endif.endif.if.1.i
  ]

for.body.endif.endif.if.1.i:                      ; preds = %for.body.endif.1.i
  %.299.1.i = icmp eq i64 %.236.3.i, -1
  %.300.1.i = select i1 %.299.1.i, i64 %.304.i, i64 %.236.3.i
  br label %for.body.endif.endif.endif.1.i

for.body.endif.endif.endif.1.i:                   ; preds = %for.body.endif.endif.if.1.i, %for.body.endif.1.i
  %.236.3.1.i = phi i64 [ %.236.3.i, %for.body.endif.1.i ], [ %.300.1.i, %for.body.endif.endif.if.1.i ]
  %.303.1.i = add i64 %.304.i, 1
  %.304.1.i = and i64 %.303.1.i, %.228.i
  %.244.2.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %.304.1.i, i32 0
  %.245.2.i = load i64, i64* %.244.2.i, align 8, !noalias !17
  %.246.2.i = icmp eq i64 %spec.select.i, %.245.2.i
  br i1 %.246.2.i, label %for.body.if.2.i, label %for.body.endif.2.i

for.body.if.2.i:                                  ; preds = %for.body.endif.endif.endif.1.i
  %.249.elt.2.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %.304.1.i, i32 1, i32 0
  %.249.unpack.2.i = load i8*, i8** %.249.elt.2.i, align 8, !noalias !25
  %.249.elt167.2.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %.304.1.i, i32 1, i32 1
  %.249.unpack168.2.i = load i64, i64* %.249.elt167.2.i, align 8, !noalias !17
  %.249.elt169.2.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %.304.1.i, i32 1, i32 2
  %.249.unpack170.2.i = load i32, i32* %.249.elt169.2.i, align 8, !noalias !25
  %.192.not.i5.2.i = icmp eq i64 %.249.unpack168.2.i, 1
  br i1 %.192.not.i5.2.i, label %B78.endif.i8.2.i, label %for.body.endif.2.i

B78.endif.i8.2.i:                                 ; preds = %for.body.if.2.i
  %.93.i12.i517.i = bitcast i8* %.249.unpack.2.i to i32*
  %.64.i7.i518.i = bitcast i8* %.249.unpack.2.i to i16*
  %32 = icmp eq i32 %.249.unpack170.2.i, 1
  br i1 %32, label %B60.us.us.i556.i.critedge, label %B58.us.preheader.i519.i

B58.us.preheader.i519.i:                          ; preds = %B78.endif.i8.2.i
  %.23.i2.i516.i = sext i32 %.249.unpack170.2.i to i64
  switch i64 %.23.i2.i516.i, label %B60.us.i567.i.critedge [
    i64 4, label %B60.us.us83.i530.i.critedge
    i64 2, label %B60.us.us96.i544.i.critedge
  ]

B60.us.us83.i530.i.critedge:                      ; preds = %B58.us.preheader.i519.i
  %.95.i14.us.us.i527.i = load i32, i32* %.93.i12.i517.i, align 4, !noalias !86
  %33 = zext i8 %.483.i.i to i32
  %.not65 = icmp eq i32 %.95.i14.us.us.i527.i, %33
  br i1 %.not65, label %lookup.end.endif.i, label %for.body.endif.2.i

B60.us.us96.i544.i.critedge:                      ; preds = %B58.us.preheader.i519.i
  %.66.i9.us.us105.i541.i = load i16, i16* %.64.i7.i518.i, align 2, !noalias !86
  %34 = zext i8 %.483.i.i to i16
  %.not64 = icmp eq i16 %.66.i9.us.us105.i541.i, %34
  br i1 %.not64, label %lookup.end.endif.i, label %for.body.endif.2.i

B60.us.us.i556.i.critedge:                        ; preds = %B78.endif.i8.2.i
  %.37.i4.us.us.i554.i = load i8, i8* %.249.unpack.2.i, align 1, !noalias !86
  %.not66 = icmp eq i8 %.483.i.i, %.37.i4.us.us.i554.i
  br i1 %.not66, label %lookup.end.endif.i, label %for.body.endif.2.i

B60.us.i567.i.critedge:                           ; preds = %B58.us.preheader.i519.i
  %.298.us.not.i566.i = icmp eq i8 %.483.i.i, 0
  br i1 %.298.us.not.i566.i, label %lookup.end.endif.i, label %for.body.endif.2.i

for.body.endif.2.i:                               ; preds = %B60.us.us.i556.i.critedge, %B60.us.us83.i530.i.critedge, %B60.us.us96.i544.i.critedge, %B60.us.i567.i.critedge, %for.body.if.2.i, %for.body.endif.endif.endif.1.i
  switch i64 %.245.2.i, label %for.body.endif.endif.endif.2.i [
    i64 -1, label %lookup.end.if.i
    i64 -2, label %for.body.endif.endif.if.2.i
  ]

for.body.endif.endif.if.2.i:                      ; preds = %for.body.endif.2.i
  %.299.2.i = icmp eq i64 %.236.3.1.i, -1
  %.300.2.i = select i1 %.299.2.i, i64 %.304.1.i, i64 %.236.3.1.i
  br label %for.body.endif.endif.endif.2.i

for.body.endif.endif.endif.2.i:                   ; preds = %for.body.endif.endif.if.2.i, %for.body.endif.2.i
  %.236.3.2.i = phi i64 [ %.236.3.1.i, %for.body.endif.2.i ], [ %.300.2.i, %for.body.endif.endif.if.2.i ]
  %.303.2.i = add i64 %.304.1.i, 1
  %35 = zext i8 %.483.i.i to i16
  %36 = zext i8 %.483.i.i to i32
  %.298.us.not.i507.i = icmp eq i8 %.483.i.i, 0
  br i1 %.298.us.not.i507.i, label %lookup.body.i.us, label %lookup.body.i

lookup.body.i.us:                                 ; preds = %for.body.endif.endif.endif.2.i, %lookup.body.endif.endif.endif.i.us
  %.229.0.i.us = phi i64 [ %.371.i.us, %lookup.body.endif.endif.endif.i.us ], [ %spec.select.i, %for.body.endif.endif.endif.2.i ]
  %.374.pn.i.us = phi i64 [ %.374.i.us, %lookup.body.endif.endif.endif.i.us ], [ %.303.2.i, %for.body.endif.endif.endif.2.i ]
  %.236.0.i.us = phi i64 [ %.236.4.i.us, %lookup.body.endif.endif.endif.i.us ], [ %.236.3.2.i, %for.body.endif.endif.endif.2.i ]
  %.233.0.i.us = and i64 %.374.pn.i.us, %.228.i
  %.311.i.us = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %.233.0.i.us, i32 0
  %.312.i.us = load i64, i64* %.311.i.us, align 8, !noalias !17
  %.313.i.us = icmp eq i64 %spec.select.i, %.312.i.us
  br i1 %.313.i.us, label %lookup.body.if.i.us, label %lookup.body.endif.i.us

lookup.body.if.i.us:                              ; preds = %lookup.body.i.us
  %.316.elt.i.us = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %.233.0.i.us, i32 1, i32 0
  %.316.unpack.i.us = load i8*, i8** %.316.elt.i.us, align 8, !noalias !63
  %.316.elt153.i.us = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %.233.0.i.us, i32 1, i32 1
  %.316.unpack154.i.us = load i64, i64* %.316.elt153.i.us, align 8, !noalias !17
  %.316.elt155.i.us = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %6, i64 %.233.0.i.us, i32 1, i32 2
  %.316.unpack156.i.us = load i32, i32* %.316.elt155.i.us, align 8, !noalias !63
  %.192.not.i18.i.us = icmp eq i64 %.316.unpack154.i.us, 1
  br i1 %.192.not.i18.i.us, label %B78.endif.i21.i.us, label %lookup.body.endif.i.us

B78.endif.i21.i.us:                               ; preds = %lookup.body.if.i.us
  %.93.i12.i458.i.us = bitcast i8* %.316.unpack.i.us to i32*
  %.64.i7.i459.i.us = bitcast i8* %.316.unpack.i.us to i16*
  %37 = icmp eq i32 %.316.unpack156.i.us, 1
  br i1 %37, label %B60.us.us.i497.i.critedge.us, label %B58.us.preheader.i460.i.us

B58.us.preheader.i460.i.us:                       ; preds = %B78.endif.i21.i.us
  %.23.i2.i457.i.us = sext i32 %.316.unpack156.i.us to i64
  switch i64 %.23.i2.i457.i.us, label %lookup.end.endif.i [
    i64 4, label %B60.us.us83.i471.i.critedge.us
    i64 2, label %B60.us.us96.i485.i.critedge.us
  ]

B60.us.us96.i485.i.critedge.us:                   ; preds = %B58.us.preheader.i460.i.us
  %.66.i9.us.us105.i482.i.us = load i16, i16* %.64.i7.i459.i.us, align 2, !noalias !67
  %.not.us = icmp eq i16 %.66.i9.us.us105.i482.i.us, %35
  br i1 %.not.us, label %lookup.end.endif.i, label %lookup.body.endif.i.us

B60.us.us83.i471.i.critedge.us:                   ; preds = %B58.us.preheader.i460.i.us
  %.95.i14.us.us.i468.i.us = load i32, i32* %.93.i12.i458.i.us, align 4, !noalias !67
  %.not62.us = icmp eq i32 %.95.i14.us.us.i468.i.us, %36
  br i1 %.not62.us, label %lookup.end.endif.i, label %lookup.body.endif.i.us

B60.us.us.i497.i.critedge.us:                     ; preds = %B78.endif.i21.i.us
  %.37.i4.us.us.i495.i.us = load i8, i8* %.316.unpack.i.us, align 1, !noalias !67
  %.not63.us = icmp eq i8 %.37.i4.us.us.i495.i.us, 0
  br i1 %.not63.us, label %lookup.end.endif.i, label %lookup.body.endif.i.us

lookup.body.endif.i.us:                           ; preds = %B60.us.us.i497.i.critedge.us, %B60.us.us83.i471.i.critedge.us, %B60.us.us96.i485.i.critedge.us, %lookup.body.if.i.us, %lookup.body.i.us
  switch i64 %.312.i.us, label %lookup.body.endif.endif.endif.i.us [
    i64 -1, label %lookup.end.if.i
    i64 -2, label %lookup.body.endif.endif.if.i.us
  ]

lookup.body.endif.endif.if.i.us:                  ; preds = %lookup.body.endif.i.us
  %.366.i.us = icmp eq i64 %.236.0.i.us, -1
  %.367.i.us = select i1 %.366.i.us, i64 %.233.0.i.us, i64 %.236.0.i.us
  br label %lookup.body.endif.endif.endif.i.us

lookup.body.endif.endif.endif.i.us:               ; preds = %lookup.body.endif.endif.if.i.us, %lookup.body.endif.i.us
  %.236.4.i.us = phi i64 [ %.236.0.i.us, %lookup.body.endif.i.us ], [ %.367.i.us, %lookup.body.endif.endif.if.i.us ]
  %.371.i.us = lshr i64 %.229.0.i.us, 5
  %.372.i.us = mul i64 %.233.0.i.us, 5
  %.373.i.us = add nuw nsw i64 %.371.i.us, 1
  %.374.i.us = add i64 %.373.i.us, %.372.i.us
  br label %lookup.body.i.us

for.body.2.if.1.i:                                ; preds = %for.body.2.endif.endif.endif.i
  %.534.elt.1.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %.589.i, i32 1, i32 0
  %.534.unpack.1.i = load i8*, i8** %.534.elt.1.i, align 8, !noalias !17
  %.534.elt139.1.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %.589.i, i32 1, i32 1
  %.534.unpack140.1.i = load i64, i64* %.534.elt139.1.i, align 8, !noalias !17
  %.534.elt141.1.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %.589.i, i32 1, i32 2
  %.534.unpack142.1.i = load i32, i32* %.534.elt141.1.i, align 8, !noalias !17
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %4) #7, !noalias !17
  store i64 0, i64* %.230.i29.i, align 8, !noalias !73
  %.192.not.i31.1.i = icmp eq i64 %.502.unpack101.i, %.534.unpack140.1.i
  br i1 %.192.not.i31.1.i, label %B78.endif.i34.1.i, label %for.body.2.if.endif.endif.thread283.1.i

for.body.2.if.endif.endif.thread283.1.i:          ; preds = %for.body.2.if.1.i
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %4) #7, !noalias !17
  br label %for.body.2.endif.1.i

B78.endif.i34.1.i:                                ; preds = %for.body.2.if.1.i
  store i64 0, i64* %.230.i29.i, align 8, !noalias !73
  call fastcc void @_ZN5numba7cpython7unicode11_cmp_regionB3v35B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex12unicode_typexx(i64* nonnull %.230.i29.i, i8* %.502.unpack.i, i64 %.502.unpack101.i, i32 %.502.unpack103.i, i8* %.534.unpack.1.i, i64 %.502.unpack101.i, i32 %.534.unpack142.1.i, i64 %.502.unpack101.i) #7
  %.244.i36.1.i = load i64, i64* %.230.i29.i, align 8, !noalias !73
  %.260.i37.not.1.i = icmp eq i64 %.244.i36.1.i, 0
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %4) #7, !noalias !17
  br i1 %.260.i37.not.1.i, label %for.body.1.endif.i, label %for.body.2.endif.1.i

for.body.2.endif.1.i:                             ; preds = %B78.endif.i34.1.i, %for.body.2.if.endif.endif.thread283.1.i, %for.body.2.endif.endif.endif.i
  switch i64 %.530.1.i, label %for.body.2.endif.endif.endif.1.i [
    i64 -1, label %lookup.end.1.if.i
    i64 -2, label %for.body.2.endif.endif.if.1.i
  ]

for.body.2.endif.endif.if.1.i:                    ; preds = %for.body.2.endif.1.i
  %.584.1.i = icmp eq i64 %.521.3.i, -1
  %.585.1.i = select i1 %.584.1.i, i64 %.589.i, i64 %.521.3.i
  br label %for.body.2.endif.endif.endif.1.i

for.body.2.endif.endif.endif.1.i:                 ; preds = %for.body.2.endif.endif.if.1.i, %for.body.2.endif.1.i
  %.521.3.1.i = phi i64 [ %.521.3.i, %for.body.2.endif.1.i ], [ %.585.1.i, %for.body.2.endif.endif.if.1.i ]
  %.588.1.i = add nuw i64 %.589.i, 1
  %.589.1.i = and i64 %.588.1.i, %.513.i
  %.529.2.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %.589.1.i, i32 0
  %.530.2.i = load i64, i64* %.529.2.i, align 8, !noalias !17
  %.531.2.i = icmp eq i64 %.498.i, %.530.2.i
  br i1 %.531.2.i, label %for.body.2.if.2.i, label %for.body.2.endif.2.i

for.body.2.if.2.i:                                ; preds = %for.body.2.endif.endif.endif.1.i
  %.534.elt.2.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %.589.1.i, i32 1, i32 0
  %.534.unpack.2.i = load i8*, i8** %.534.elt.2.i, align 8, !noalias !17
  %.534.elt139.2.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %.589.1.i, i32 1, i32 1
  %.534.unpack140.2.i = load i64, i64* %.534.elt139.2.i, align 8, !noalias !17
  %.534.elt141.2.i = getelementptr { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }, { i64, { i8*, i64, i32, i32, i64, i8*, i8* } }* %20, i64 %.589.1.i, i32 1, i32 2
  %.534.unpack142.2.i = load i32, i32* %.534.elt141.2.i, align 8, !noalias !17
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %4) #7, !noalias !17
  store i64 0, i64* %.230.i29.i, align 8, !noalias !73
  %.192.not.i31.2.i = icmp eq i64 %.502.unpack101.i, %.534.unpack140.2.i
  br i1 %.192.not.i31.2.i, label %B78.endif.i34.2.i, label %for.body.2.if.endif.endif.thread283.2.i

for.body.2.if.endif.endif.thread283.2.i:          ; preds = %for.body.2.if.2.i
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %4) #7, !noalias !17
  br label %for.body.2.endif.2.i

B78.endif.i34.2.i:                                ; preds = %for.body.2.if.2.i
  store i64 0, i64* %.230.i29.i, align 8, !noalias !73
  call fastcc void @_ZN5numba7cpython7unicode11_cmp_regionB3v35B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex12unicode_typexx(i64* nonnull %.230.i29.i, i8* %.502.unpack.i, i64 %.502.unpack101.i, i32 %.502.unpack103.i, i8* %.534.unpack.2.i, i64 %.502.unpack101.i, i32 %.534.unpack142.2.i, i64 %.502.unpack101.i) #7
  %.244.i36.2.i = load i64, i64* %.230.i29.i, align 8, !noalias !73
  %.260.i37.not.2.i = icmp eq i64 %.244.i36.2.i, 0
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %4) #7, !noalias !17
  br i1 %.260.i37.not.2.i, label %for.body.1.endif.i, label %for.body.2.endif.2.i

for.body.2.endif.2.i:                             ; preds = %B78.endif.i34.2.i, %for.body.2.if.endif.endif.thread283.2.i, %for.body.2.endif.endif.endif.1.i
  switch i64 %.530.2.i, label %for.body.2.endif.endif.endif.2.i [
    i64 -1, label %lookup.end.1.if.i
    i64 -2, label %for.body.2.endif.endif.if.2.i
  ]

for.body.2.endif.endif.if.2.i:                    ; preds = %for.body.2.endif.2.i
  %.584.2.i = icmp eq i64 %.521.3.1.i, -1
  %.585.2.i = select i1 %.584.2.i, i64 %.589.1.i, i64 %.521.3.1.i
  br label %for.body.2.endif.endif.endif.2.i

for.body.2.endif.endif.endif.2.i:                 ; preds = %for.body.2.endif.endif.if.2.i, %for.body.2.endif.2.i
  %.521.3.2.i = phi i64 [ %.521.3.1.i, %for.body.2.endif.2.i ], [ %.585.2.i, %for.body.2.endif.endif.if.2.i ]
  %.588.2.i = add i64 %.589.1.i, 1
  br label %lookup.body.1.i

_ZN3rbc5tests7heavydb8test_nrt15test_set_simple12_3clocals_3e2fnB2v1B44c8tJTC_2fWQA9wW1DkAz0Pj1skAdT4gkkUlYBZmgA_3dE73TextEncodingNonePointer_5bSTRUCT__cPW3WptrLW2WszV5Vbool8W7Wis_nullK_5d_2a.exit: ; preds = %lookup.end.endif.i
  %.720.i = tail call fastcc i8* @NRT_MemInfo_data_fast(i8* nonnull %0) #7, !noalias !17
  %.723.i = getelementptr inbounds i8, i8* %.720.i, i64 8
  %38 = bitcast i8* %.723.i to i64*
  %.724.i = load i64, i64* %38, align 8, !noalias !17
  %.726.i = trunc i64 %.724.i to i32
  br label %entry.endif

entry.if:                                         ; preds = %for_iter.body.endif.if.i, %calcsize.end.endif.if.i, %calcsize.end.i, %entry
  %.20 = tail call i32 (i8*, ...) @printf(i8* nonnull dereferenceable(1) getelementptr inbounds ([43 x i8], [43 x i8]* @printf_format.79, i64 0, i64 0), i32 1)
  %.21 = tail call i32 @fflush(i8* null)
  br label %entry.endif

entry.endif:                                      ; preds = %_ZN3rbc5tests7heavydb8test_nrt15test_set_simple12_3clocals_3e2fnB2v1B44c8tJTC_2fWQA9wW1DkAz0Pj1skAdT4gkkUlYBZmgA_3dE73TextEncodingNonePointer_5bSTRUCT__cPW3WptrLW2WszV5Vbool8W7Wis_nullK_5d_2a.exit, %entry.if
  %.3.060 = phi i32 [ %.726.i, %_ZN3rbc5tests7heavydb8test_nrt15test_set_simple12_3clocals_3e2fnB2v1B44c8tJTC_2fWQA9wW1DkAz0Pj1skAdT4gkkUlYBZmgA_3dE73TextEncodingNonePointer_5bSTRUCT__cPW3WptrLW2WszV5Vbool8W7Wis_nullK_5d_2a.exit ], [ 0, %entry.if ]
  ret i32 %.3.060
}

; Function Attrs: nofree nounwind
declare i32 @fflush(i8* nocapture) local_unnamed_addr #2

; Function Attrs: nofree norecurse nounwind
define private fastcc void @_ZN5numba7cpython7unicode11_cmp_regionB3v35B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex12unicode_typexx(i64* noalias nocapture %retptr, i8* nocapture readonly %arg.a.0, i64 %arg.a.1, i32 %arg.a.2, i8* nocapture readonly %arg.b.0, i64 %arg.b.1, i32 %arg.b.2, i64 %arg.n) unnamed_addr #8 {
entry:
  %.32 = icmp eq i64 %arg.n, 0
  br i1 %.32, label %B10, label %B14

B10:                                              ; preds = %entry
  store i64 0, i64* %retptr, align 8
  ret void

B14:                                              ; preds = %entry
  %.56 = icmp sgt i64 %arg.n, %arg.a.1
  br i1 %.56, label %B28, label %B32

B28:                                              ; preds = %B14
  store i64 -1, i64* %retptr, align 8
  ret void

B32:                                              ; preds = %B14
  %.80 = icmp sgt i64 %arg.n, %arg.b.1
  br i1 %.80, label %B46, label %B50

B46:                                              ; preds = %B32
  store i64 1, i64* %retptr, align 8
  ret void

B50:                                              ; preds = %B32
  %.23.i = sext i32 %arg.a.2 to i64
  %.93.i = bitcast i8* %arg.a.0 to i32*
  %.64.i = bitcast i8* %arg.a.0 to i16*
  %.23.i2 = sext i32 %arg.b.2 to i64
  %.93.i12 = bitcast i8* %arg.b.0 to i32*
  %.64.i7 = bitcast i8* %arg.b.0 to i16*
  %.134.inv = icmp sgt i64 %arg.n, 0
  %spec.select = select i1 %.134.inv, i64 %arg.n, i64 0
  switch i64 %.23.i, label %B58.preheader [
    i64 1, label %B50.split.us
    i64 2, label %B58.us32.preheader
    i64 4, label %B58.us71.preheader
  ]

B58.us71.preheader:                               ; preds = %B50
  %0 = icmp eq i32 %arg.b.2, 1
  br i1 %0, label %B58.us71.us, label %B58.us71.preheader56

B58.us71.preheader56:                             ; preds = %B58.us71.preheader
  switch i64 %.23.i2, label %B58.us71 [
    i64 4, label %B58.us71.us96
    i64 2, label %B58.us71.us110
  ]

B58.us71.us96:                                    ; preds = %B58.us71.preheader56, %B104.us101.us107
  %.120.0.us72.us97 = phi i64 [ %.191.us77.us100, %B104.us101.us107 ], [ 0, %B58.us71.preheader56 ]
  %exitcond127.not.us98 = icmp eq i64 %.120.0.us72.us97, %spec.select
  br i1 %exitcond127.not.us98, label %B120, label %B60.us75.us99

B60.us75.us99:                                    ; preds = %B58.us71.us96
  %.94.i.us80.us102 = getelementptr i32, i32* %.93.i, i64 %.120.0.us72.us97
  %.95.i.us81.us103 = load i32, i32* %.94.i.us80.us102, align 4, !noalias !91
  %.94.i13.us88.us = getelementptr i32, i32* %.93.i12, i64 %.120.0.us72.us97
  %.95.i14.us89.us = load i32, i32* %.94.i13.us88.us, align 4, !noalias !94
  %.278.us100.us106 = icmp ugt i32 %.95.i14.us89.us, %.95.i.us81.us103
  br i1 %.278.us100.us106, label %B98, label %B104.us101.us107

B104.us101.us107:                                 ; preds = %B60.us75.us99
  %.191.us77.us100 = add nuw i64 %.120.0.us72.us97, 1
  %.298.us102.us108 = icmp ult i32 %.95.i14.us89.us, %.95.i.us81.us103
  br i1 %.298.us102.us108, label %B112, label %B58.us71.us96

B58.us71.us110:                                   ; preds = %B58.us71.preheader56, %B104.us101.us127
  %.120.0.us72.us111 = phi i64 [ %.191.us77.us114, %B104.us101.us127 ], [ 0, %B58.us71.preheader56 ]
  %exitcond127.not.us112 = icmp eq i64 %.120.0.us72.us111, %spec.select
  br i1 %exitcond127.not.us112, label %B120, label %B60.us75.us113

B60.us75.us113:                                   ; preds = %B58.us71.us110
  %.94.i.us80.us116 = getelementptr i32, i32* %.93.i, i64 %.120.0.us72.us111
  %.95.i.us81.us117 = load i32, i32* %.94.i.us80.us116, align 4, !noalias !91
  %.98.i.us82.us118 = zext i32 %.95.i.us81.us117 to i64
  %.65.i8.us92.us121 = getelementptr i16, i16* %.64.i7, i64 %.120.0.us72.us111
  %.66.i9.us93.us122 = load i16, i16* %.65.i8.us92.us121, align 2, !noalias !94
  %.70.i10.us94.us123 = zext i16 %.66.i9.us93.us122 to i64
  %.278.us100.us126 = icmp ugt i64 %.70.i10.us94.us123, %.98.i.us82.us118
  br i1 %.278.us100.us126, label %B98, label %B104.us101.us127

B104.us101.us127:                                 ; preds = %B60.us75.us113
  %.191.us77.us114 = add nuw i64 %.120.0.us72.us111, 1
  %.298.us102.us128 = icmp ult i64 %.70.i10.us94.us123, %.98.i.us82.us118
  br i1 %.298.us102.us128, label %B112, label %B58.us71.us110

B58.us71.us:                                      ; preds = %B58.us71.preheader, %B104.us101.us
  %.120.0.us72.us = phi i64 [ %.191.us77.us, %B104.us101.us ], [ 0, %B58.us71.preheader ]
  %exitcond127.not.us = icmp eq i64 %.120.0.us72.us, %spec.select
  br i1 %exitcond127.not.us, label %B120, label %B60.us75.us

B60.us75.us:                                      ; preds = %B58.us71.us
  %.94.i.us80.us = getelementptr i32, i32* %.93.i, i64 %.120.0.us72.us
  %.95.i.us81.us = load i32, i32* %.94.i.us80.us, align 4, !noalias !91
  %.98.i.us82.us = zext i32 %.95.i.us81.us to i64
  %.36.i3.us96.us = getelementptr i8, i8* %arg.b.0, i64 %.120.0.us72.us
  %.37.i4.us97.us = load i8, i8* %.36.i3.us96.us, align 1, !noalias !94
  %.41.i5.us98.us = zext i8 %.37.i4.us97.us to i64
  %.278.us100.us = icmp ugt i64 %.41.i5.us98.us, %.98.i.us82.us
  br i1 %.278.us100.us, label %B98, label %B104.us101.us

B104.us101.us:                                    ; preds = %B60.us75.us
  %.191.us77.us = add nuw i64 %.120.0.us72.us, 1
  %.298.us102.us = icmp ult i64 %.41.i5.us98.us, %.98.i.us82.us
  br i1 %.298.us102.us, label %B112, label %B58.us71.us

B58.us32.preheader:                               ; preds = %B50
  %1 = icmp eq i32 %arg.b.2, 1
  br i1 %1, label %B58.us32.us, label %B58.us32.preheader49

B58.us32.preheader49:                             ; preds = %B58.us32.preheader
  switch i64 %.23.i2, label %B58.us32 [
    i64 4, label %B58.us32.us60
    i64 2, label %B58.us32.us73
  ]

B58.us32.us60:                                    ; preds = %B58.us32.preheader49, %B104.us65.us71
  %.120.0.us33.us61 = phi i64 [ %.191.us38.us64, %B104.us65.us71 ], [ 0, %B58.us32.preheader49 ]
  %exitcond126.not.us62 = icmp eq i64 %.120.0.us33.us61, %spec.select
  br i1 %exitcond126.not.us62, label %B120, label %B60.us36.us63

B60.us36.us63:                                    ; preds = %B58.us32.us60
  %.65.i.us45.us66 = getelementptr i16, i16* %.64.i, i64 %.120.0.us33.us61
  %.66.i.us46.us67 = load i16, i16* %.65.i.us45.us66, align 2, !noalias !91
  %.70.i.us47.us68 = zext i16 %.66.i.us46.us67 to i64
  %.94.i13.us52.us = getelementptr i32, i32* %.93.i12, i64 %.120.0.us33.us61
  %.95.i14.us53.us = load i32, i32* %.94.i13.us52.us, align 4, !noalias !94
  %.98.i15.us54.us = zext i32 %.95.i14.us53.us to i64
  %.278.us64.us70 = icmp ugt i64 %.98.i15.us54.us, %.70.i.us47.us68
  br i1 %.278.us64.us70, label %B98, label %B104.us65.us71

B104.us65.us71:                                   ; preds = %B60.us36.us63
  %.191.us38.us64 = add nuw i64 %.120.0.us33.us61, 1
  %.298.us66.us72 = icmp ult i64 %.98.i15.us54.us, %.70.i.us47.us68
  br i1 %.298.us66.us72, label %B112, label %B58.us32.us60

B58.us32.us73:                                    ; preds = %B58.us32.preheader49, %B104.us65.us90
  %.120.0.us33.us74 = phi i64 [ %.191.us38.us77, %B104.us65.us90 ], [ 0, %B58.us32.preheader49 ]
  %exitcond126.not.us75 = icmp eq i64 %.120.0.us33.us74, %spec.select
  br i1 %exitcond126.not.us75, label %B120, label %B60.us36.us76

B60.us36.us76:                                    ; preds = %B58.us32.us73
  %.65.i.us45.us79 = getelementptr i16, i16* %.64.i, i64 %.120.0.us33.us74
  %.66.i.us46.us80 = load i16, i16* %.65.i.us45.us79, align 2, !noalias !91
  %.65.i8.us56.us84 = getelementptr i16, i16* %.64.i7, i64 %.120.0.us33.us74
  %.66.i9.us57.us85 = load i16, i16* %.65.i8.us56.us84, align 2, !noalias !94
  %.278.us64.us89 = icmp ugt i16 %.66.i9.us57.us85, %.66.i.us46.us80
  br i1 %.278.us64.us89, label %B98, label %B104.us65.us90

B104.us65.us90:                                   ; preds = %B60.us36.us76
  %.191.us38.us77 = add nuw i64 %.120.0.us33.us74, 1
  %.298.us66.us91 = icmp ult i16 %.66.i9.us57.us85, %.66.i.us46.us80
  br i1 %.298.us66.us91, label %B112, label %B58.us32.us73

B58.us32.us:                                      ; preds = %B58.us32.preheader, %B104.us65.us
  %.120.0.us33.us = phi i64 [ %.191.us38.us, %B104.us65.us ], [ 0, %B58.us32.preheader ]
  %exitcond126.not.us = icmp eq i64 %.120.0.us33.us, %spec.select
  br i1 %exitcond126.not.us, label %B120, label %B60.us36.us

B60.us36.us:                                      ; preds = %B58.us32.us
  %.65.i.us45.us = getelementptr i16, i16* %.64.i, i64 %.120.0.us33.us
  %.66.i.us46.us = load i16, i16* %.65.i.us45.us, align 2, !noalias !91
  %.70.i.us47.us = zext i16 %.66.i.us46.us to i64
  %.36.i3.us60.us = getelementptr i8, i8* %arg.b.0, i64 %.120.0.us33.us
  %.37.i4.us61.us = load i8, i8* %.36.i3.us60.us, align 1, !noalias !94
  %.41.i5.us62.us = zext i8 %.37.i4.us61.us to i64
  %.278.us64.us = icmp ugt i64 %.41.i5.us62.us, %.70.i.us47.us
  br i1 %.278.us64.us, label %B98, label %B104.us65.us

B104.us65.us:                                     ; preds = %B60.us36.us
  %.191.us38.us = add nuw i64 %.120.0.us33.us, 1
  %.298.us66.us = icmp ult i64 %.41.i5.us62.us, %.70.i.us47.us
  br i1 %.298.us66.us, label %B112, label %B58.us32.us

B58.preheader:                                    ; preds = %B50
  %exitcond.not154 = icmp slt i64 %arg.n, 1
  br i1 %exitcond.not154, label %B120, label %B60.preheader

B60.preheader:                                    ; preds = %B58.preheader
  switch i64 %.23.i2, label %B120 [
    i64 1, label %B60.us25
    i64 2, label %B60.us38
    i64 4, label %B60.us59
  ]

B60.us25:                                         ; preds = %B60.preheader, %B104.us37
  %.120.0155.us = phi i64 [ %.191.us26, %B104.us37 ], [ 0, %B60.preheader ]
  %.191.us26 = add nuw nsw i64 %.120.0155.us, 1
  %.36.i3.us = getelementptr i8, i8* %arg.b.0, i64 %.120.0155.us
  %.37.i4.us = load i8, i8* %.36.i3.us, align 1, !noalias !94
  %.278.not.us = icmp eq i8 %.37.i4.us, 0
  br i1 %.278.not.us, label %B104.us37, label %B98

B104.us37:                                        ; preds = %B60.us25
  %exitcond.not.us = icmp eq i64 %.191.us26, %spec.select
  br i1 %exitcond.not.us, label %B120, label %B60.us25

B60.us38:                                         ; preds = %B60.preheader, %B104.us53
  %.120.0155.us39 = phi i64 [ %.191.us40, %B104.us53 ], [ 0, %B60.preheader ]
  %.191.us40 = add nuw nsw i64 %.120.0155.us39, 1
  %.65.i8.us47 = getelementptr i16, i16* %.64.i7, i64 %.120.0155.us39
  %.66.i9.us48 = load i16, i16* %.65.i8.us47, align 2, !noalias !94
  %.278.not.us52 = icmp eq i16 %.66.i9.us48, 0
  br i1 %.278.not.us52, label %B104.us53, label %B98

B104.us53:                                        ; preds = %B60.us38
  %exitcond.not.us54 = icmp eq i64 %.191.us40, %spec.select
  br i1 %exitcond.not.us54, label %B120, label %B60.us38

B60.us59:                                         ; preds = %B60.preheader, %B104.us71
  %.120.0155.us60 = phi i64 [ %.191.us61, %B104.us71 ], [ 0, %B60.preheader ]
  %.191.us61 = add nuw nsw i64 %.120.0155.us60, 1
  %.94.i13.us64 = getelementptr i32, i32* %.93.i12, i64 %.120.0155.us60
  %.95.i14.us65 = load i32, i32* %.94.i13.us64, align 4, !noalias !94
  %.278.not.us70 = icmp eq i32 %.95.i14.us65, 0
  br i1 %.278.not.us70, label %B104.us71, label %B98

B104.us71:                                        ; preds = %B60.us59
  %exitcond.not.us72 = icmp eq i64 %.191.us61, %spec.select
  br i1 %exitcond.not.us72, label %B120, label %B60.us59

B50.split.us:                                     ; preds = %B50
  %2 = icmp eq i32 %arg.b.2, 1
  br i1 %2, label %B58.us.us, label %B58.us.preheader

B58.us.preheader:                                 ; preds = %B50.split.us
  switch i64 %.23.i2, label %B58.us [
    i64 4, label %B58.us.us81
    i64 2, label %B58.us.us93
  ]

B58.us.us81:                                      ; preds = %B58.us.preheader, %B104.us.us90
  %.120.0.us.us82 = phi i64 [ %.191.us.us84, %B104.us.us90 ], [ 0, %B58.us.preheader ]
  %exitcond125.not.us = icmp eq i64 %.120.0.us.us82, %spec.select
  br i1 %exitcond125.not.us, label %B120, label %B60.us.us83

B60.us.us83:                                      ; preds = %B58.us.us81
  %.36.i.us.us86 = getelementptr i8, i8* %arg.a.0, i64 %.120.0.us.us82
  %.37.i.us.us87 = load i8, i8* %.36.i.us.us86, align 1, !noalias !91
  %.41.i.us.us = zext i8 %.37.i.us.us87 to i64
  %.94.i13.us.us = getelementptr i32, i32* %.93.i12, i64 %.120.0.us.us82
  %.95.i14.us.us = load i32, i32* %.94.i13.us.us, align 4, !noalias !94
  %.98.i15.us.us = zext i32 %.95.i14.us.us to i64
  %.278.us.us89 = icmp ugt i64 %.98.i15.us.us, %.41.i.us.us
  br i1 %.278.us.us89, label %B98, label %B104.us.us90

B104.us.us90:                                     ; preds = %B60.us.us83
  %.191.us.us84 = add nuw i64 %.120.0.us.us82, 1
  %.298.us.us91 = icmp ult i64 %.98.i15.us.us, %.41.i.us.us
  br i1 %.298.us.us91, label %B112, label %B58.us.us81

B58.us.us93:                                      ; preds = %B58.us.preheader, %B104.us.us110
  %.120.0.us.us94 = phi i64 [ %.191.us.us97, %B104.us.us110 ], [ 0, %B58.us.preheader ]
  %exitcond125.not.us95 = icmp eq i64 %.120.0.us.us94, %spec.select
  br i1 %exitcond125.not.us95, label %B120, label %B60.us.us96

B60.us.us96:                                      ; preds = %B58.us.us93
  %.36.i.us.us99 = getelementptr i8, i8* %arg.a.0, i64 %.120.0.us.us94
  %.37.i.us.us100 = load i8, i8* %.36.i.us.us99, align 1, !noalias !91
  %.41.i.us.us101 = zext i8 %.37.i.us.us100 to i64
  %.65.i8.us.us104 = getelementptr i16, i16* %.64.i7, i64 %.120.0.us.us94
  %.66.i9.us.us105 = load i16, i16* %.65.i8.us.us104, align 2, !noalias !94
  %.70.i10.us.us106 = zext i16 %.66.i9.us.us105 to i64
  %.278.us.us109 = icmp ugt i64 %.70.i10.us.us106, %.41.i.us.us101
  br i1 %.278.us.us109, label %B98, label %B104.us.us110

B104.us.us110:                                    ; preds = %B60.us.us96
  %.191.us.us97 = add nuw i64 %.120.0.us.us94, 1
  %.298.us.us111 = icmp ult i64 %.70.i10.us.us106, %.41.i.us.us101
  br i1 %.298.us.us111, label %B112, label %B58.us.us93

B58.us.us:                                        ; preds = %B50.split.us, %B104.us.us
  %.120.0.us.us = phi i64 [ %.191.us.us, %B104.us.us ], [ 0, %B50.split.us ]
  %exitcond124.not = icmp eq i64 %.120.0.us.us, %spec.select
  br i1 %exitcond124.not, label %B120, label %B60.us.us

B60.us.us:                                        ; preds = %B58.us.us
  %.36.i.us.us = getelementptr i8, i8* %arg.a.0, i64 %.120.0.us.us
  %.37.i.us.us = load i8, i8* %.36.i.us.us, align 1, !noalias !91
  %.36.i3.us.us = getelementptr i8, i8* %arg.b.0, i64 %.120.0.us.us
  %.37.i4.us.us = load i8, i8* %.36.i3.us.us, align 1, !noalias !94
  %.278.us.us = icmp ult i8 %.37.i.us.us, %.37.i4.us.us
  br i1 %.278.us.us, label %B98, label %B104.us.us

B104.us.us:                                       ; preds = %B60.us.us
  %.191.us.us = add nuw i64 %.120.0.us.us, 1
  %.298.us.us = icmp ugt i8 %.37.i.us.us, %.37.i4.us.us
  br i1 %.298.us.us, label %B112, label %B58.us.us

B58.us:                                           ; preds = %B58.us.preheader, %B60.us
  %.120.0.us = phi i64 [ %.191.us, %B60.us ], [ 0, %B58.us.preheader ]
  %exitcond125.not = icmp eq i64 %.120.0.us, %spec.select
  br i1 %exitcond125.not, label %B120, label %B60.us

B60.us:                                           ; preds = %B58.us
  %.191.us = add nuw i64 %.120.0.us, 1
  %.36.i.us = getelementptr i8, i8* %arg.a.0, i64 %.120.0.us
  %.37.i.us = load i8, i8* %.36.i.us, align 1, !noalias !91
  %.298.us.not = icmp eq i8 %.37.i.us, 0
  br i1 %.298.us.not, label %B58.us, label %B112

B58.us32:                                         ; preds = %B58.us32.preheader49, %B60.us36
  %.120.0.us33 = phi i64 [ %.191.us38, %B60.us36 ], [ 0, %B58.us32.preheader49 ]
  %exitcond126.not = icmp eq i64 %.120.0.us33, %spec.select
  br i1 %exitcond126.not, label %B120, label %B60.us36

B60.us36:                                         ; preds = %B58.us32
  %.191.us38 = add nuw i64 %.120.0.us33, 1
  %.65.i.us45 = getelementptr i16, i16* %.64.i, i64 %.120.0.us33
  %.66.i.us46 = load i16, i16* %.65.i.us45, align 2, !noalias !91
  %.298.us66.not = icmp eq i16 %.66.i.us46, 0
  br i1 %.298.us66.not, label %B58.us32, label %B112

B58.us71:                                         ; preds = %B58.us71.preheader56, %B60.us75
  %.120.0.us72 = phi i64 [ %.191.us77, %B60.us75 ], [ 0, %B58.us71.preheader56 ]
  %exitcond127.not = icmp eq i64 %.120.0.us72, %spec.select
  br i1 %exitcond127.not, label %B120, label %B60.us75

B60.us75:                                         ; preds = %B58.us71
  %.191.us77 = add nuw i64 %.120.0.us72, 1
  %.94.i.us80 = getelementptr i32, i32* %.93.i, i64 %.120.0.us72
  %.95.i.us81 = load i32, i32* %.94.i.us80, align 4, !noalias !91
  %.298.us102.not = icmp eq i32 %.95.i.us81, 0
  br i1 %.298.us102.not, label %B58.us71, label %B112

B98:                                              ; preds = %B60.us75.us113, %B60.us75.us99, %B60.us75.us, %B60.us36.us76, %B60.us36.us63, %B60.us36.us, %B60.us.us96, %B60.us.us83, %B60.us.us, %B60.us59, %B60.us38, %B60.us25
  store i64 -1, i64* %retptr, align 8
  ret void

B112:                                             ; preds = %B104.us101.us127, %B104.us101.us107, %B60.us75, %B104.us101.us, %B104.us65.us90, %B104.us65.us71, %B60.us36, %B104.us65.us, %B104.us.us110, %B104.us.us90, %B60.us, %B104.us.us
  store i64 1, i64* %retptr, align 8
  ret void

B120:                                             ; preds = %B58.us71.us110, %B58.us71.us96, %B58.us71, %B58.us71.us, %B58.us32.us73, %B58.us32.us60, %B58.us32, %B58.us32.us, %B58.us.us93, %B58.us.us81, %B58.us, %B58.us.us, %B104.us71, %B104.us53, %B104.us37, %B60.preheader, %B58.preheader
  store i64 0, i64* %retptr, align 8
  ret void
}

; Function Attrs: argmemonly nounwind willreturn
declare void @llvm.lifetime.start.p0i8(i64 immarg, i8* nocapture) #9

; Function Attrs: argmemonly nounwind willreturn
declare void @llvm.lifetime.end.p0i8(i64 immarg, i8* nocapture) #9

; Function Attrs: nofree nounwind
declare i32 @puts(i8* nocapture readonly) local_unnamed_addr #2

attributes #0 = { nofree noinline nounwind }
attributes #1 = { nofree noinline norecurse nounwind }
attributes #2 = { nofree nounwind }
attributes #3 = { noinline }
attributes #4 = { norecurse nounwind readnone }
attributes #5 = { argmemonly nounwind willreturn writeonly }
attributes #6 = { nounwind readnone speculatable willreturn }
attributes #7 = { nounwind }
attributes #8 = { nofree norecurse nounwind }
attributes #9 = { argmemonly nounwind willreturn }

!numba_args_may_always_need_nrt = !{!0, !1, !2, !3, !3, !3, !3, !1, !4, !5, !6, !4, !5, !7, !7, !6, !0, !3, !8, !8, !9, !9, !9, !10, !8, !9, !10, !11, !11, !11, !2, !12, !12, !13, !13}
!llvm.module.flags = !{!14, !15}

!0 = distinct !{null}
!1 = distinct !{null}
!2 = distinct !{null}
!3 = distinct !{null}
!4 = distinct !{null}
!5 = distinct !{null}
!6 = distinct !{null}
!7 = distinct !{null}
!8 = distinct !{null}
!9 = distinct !{null}
!10 = distinct !{null}
!11 = distinct !{null}
!12 = distinct !{null}
!13 = distinct !{null}
!14 = !{i32 4, !"pass_column_arguments_by_value", i1 false}
!15 = !{i32 4, !"manage_memory_buffer", i1 true}
!16 = !{!"branch_weights", i32 1, i32 99}
!17 = !{!18, !20}
!18 = distinct !{!18, !19, !"_ZN3rbc5tests7heavydb8test_nrt15test_set_simple12_3clocals_3e2fnB2v1B44c8tJTC_2fWQA9wW1DkAz0Pj1skAdT4gkkUlYBZmgA_3dE73TextEncodingNonePointer_5bSTRUCT__cPW3WptrLW2WszV5Vbool8W7Wis_nullK_5d_2a: %retptr"}
!19 = distinct !{!19, !"_ZN3rbc5tests7heavydb8test_nrt15test_set_simple12_3clocals_3e2fnB2v1B44c8tJTC_2fWQA9wW1DkAz0Pj1skAdT4gkkUlYBZmgA_3dE73TextEncodingNonePointer_5bSTRUCT__cPW3WptrLW2WszV5Vbool8W7Wis_nullK_5d_2a"}
!20 = distinct !{!20, !19, !"_ZN3rbc5tests7heavydb8test_nrt15test_set_simple12_3clocals_3e2fnB2v1B44c8tJTC_2fWQA9wW1DkAz0Pj1skAdT4gkkUlYBZmgA_3dE73TextEncodingNonePointer_5bSTRUCT__cPW3WptrLW2WszV5Vbool8W7Wis_nullK_5d_2a: %excinfo"}
!21 = !{!22, !24, !18, !20}
!22 = distinct !{!22, !23, !"_ZN5numba7cpython7unicode15unicode_getitem12_3clocals_3e12getitem_charB2v2B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_typey: %retptr"}
!23 = distinct !{!23, !"_ZN5numba7cpython7unicode15unicode_getitem12_3clocals_3e12getitem_charB2v2B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_typey"}
!24 = distinct !{!24, !23, !"_ZN5numba7cpython7unicode15unicode_getitem12_3clocals_3e12getitem_charB2v2B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_typey: %excinfo"}
!25 = !{!26, !28, !18, !20}
!26 = distinct !{!26, !27, !"_ZN5numba7cpython7unicode10unicode_eq12_3clocals_3e7eq_implB3v33B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type12unicode_type: %retptr"}
!27 = distinct !{!27, !"_ZN5numba7cpython7unicode10unicode_eq12_3clocals_3e7eq_implB3v33B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type12unicode_type"}
!28 = distinct !{!28, !27, !"_ZN5numba7cpython7unicode10unicode_eq12_3clocals_3e7eq_implB3v33B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type12unicode_type: %excinfo"}
!29 = !{!30, !32, !33, !35, !22, !24, !18, !20}
!30 = distinct !{!30, !31, !"_ZN5numba7cpython7unicode15_set_code_pointB3v10B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typexj: %retptr"}
!31 = distinct !{!31, !"_ZN5numba7cpython7unicode15_set_code_pointB3v10B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typexj"}
!32 = distinct !{!32, !31, !"_ZN5numba7cpython7unicode15_set_code_pointB3v10B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typexj: %excinfo"}
!33 = distinct !{!33, !34, !"_ZN5numba7cpython7unicode13_empty_stringB2v8B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dExxb: %retptr"}
!34 = distinct !{!34, !"_ZN5numba7cpython7unicode13_empty_stringB2v8B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dExxb"}
!35 = distinct !{!35, !34, !"_ZN5numba7cpython7unicode13_empty_stringB2v8B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dExxb: %excinfo"}
!36 = !{!37, !39, !22, !24, !18, !20}
!37 = distinct !{!37, !38, !"_ZN5numba7cpython7unicode15_set_code_pointB3v11B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typexx: %retptr"}
!38 = distinct !{!38, !"_ZN5numba7cpython7unicode15_set_code_pointB3v11B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typexx"}
!39 = distinct !{!39, !38, !"_ZN5numba7cpython7unicode15_set_code_pointB3v11B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typexx: %excinfo"}
!40 = !{!41, !43, !45, !46, !48, !49, !51, !18, !20}
!41 = distinct !{!41, !42, !"_ZN5numba7cpython7hashing21_impl_load_hashsecret12_3clocals_3e3impB3v22B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE32Literal_5bstr_5d_28siphash_k0_29: %retptr"}
!42 = distinct !{!42, !"_ZN5numba7cpython7hashing21_impl_load_hashsecret12_3clocals_3e3impB3v22B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE32Literal_5bstr_5d_28siphash_k0_29"}
!43 = distinct !{!43, !44, !"_ZN5numba7cpython7hashing13_Py_HashBytesB3v16B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE7void_2ax: %retptr"}
!44 = distinct !{!44, !"_ZN5numba7cpython7hashing13_Py_HashBytesB3v16B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE7void_2ax"}
!45 = distinct !{!45, !44, !"_ZN5numba7cpython7hashing13_Py_HashBytesB3v16B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE7void_2ax: %excinfo"}
!46 = distinct !{!46, !47, !"_ZN5numba7cpython7hashing12unicode_hash12_3clocals_3e4implB3v14B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type: %retptr"}
!47 = distinct !{!47, !"_ZN5numba7cpython7hashing12unicode_hash12_3clocals_3e4implB3v14B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type"}
!48 = distinct !{!48, !47, !"_ZN5numba7cpython7hashing12unicode_hash12_3clocals_3e4implB3v14B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type: %excinfo"}
!49 = distinct !{!49, !50, !"_ZN5numba7cpython7hashing13hash_overload12_3clocals_3e4implB3v13B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type: %retptr"}
!50 = distinct !{!50, !"_ZN5numba7cpython7hashing13hash_overload12_3clocals_3e4implB3v13B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type"}
!51 = distinct !{!51, !50, !"_ZN5numba7cpython7hashing13hash_overload12_3clocals_3e4implB3v13B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type: %excinfo"}
!52 = !{!53, !43, !45, !46, !48, !49, !51, !18, !20}
!53 = distinct !{!53, !54, !"_ZN5numba7cpython7hashing21_impl_load_hashsecret12_3clocals_3e3impB3v24B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE32Literal_5bstr_5d_28siphash_k1_29: %retptr"}
!54 = distinct !{!54, !"_ZN5numba7cpython7hashing21_impl_load_hashsecret12_3clocals_3e3impB3v24B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE32Literal_5bstr_5d_28siphash_k1_29"}
!55 = !{!56, !18, !20}
!56 = distinct !{!56, !57, !"_ZN5numba7cpython7hashing10_siphash24B3v25B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dEyy7void_2ax: %retptr"}
!57 = distinct !{!57, !"_ZN5numba7cpython7hashing10_siphash24B3v25B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dEyy7void_2ax"}
!58 = !{!59, !26, !28, !61, !18, !20}
!59 = distinct !{!59, !60, !"_ZN5numba7cpython7unicode15_get_code_pointB3v36B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex: %retptr"}
!60 = distinct !{!60, !"_ZN5numba7cpython7unicode15_get_code_pointB3v36B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex"}
!61 = distinct !{!61, !62, !"_ZN5numba7cpython7unicode11_cmp_regionB3v35B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex12unicode_typexx: %retptr"}
!62 = distinct !{!62, !"_ZN5numba7cpython7unicode11_cmp_regionB3v35B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex12unicode_typexx"}
!63 = !{!64, !66, !18, !20}
!64 = distinct !{!64, !65, !"_ZN5numba7cpython7unicode10unicode_eq12_3clocals_3e7eq_implB3v33B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type12unicode_type: %retptr"}
!65 = distinct !{!65, !"_ZN5numba7cpython7unicode10unicode_eq12_3clocals_3e7eq_implB3v33B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type12unicode_type"}
!66 = distinct !{!66, !65, !"_ZN5numba7cpython7unicode10unicode_eq12_3clocals_3e7eq_implB3v33B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type12unicode_type: %excinfo"}
!67 = !{!68, !64, !66, !70, !18, !20}
!68 = distinct !{!68, !69, !"_ZN5numba7cpython7unicode15_get_code_pointB3v36B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex: %retptr"}
!69 = distinct !{!69, !"_ZN5numba7cpython7unicode15_get_code_pointB3v36B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex"}
!70 = distinct !{!70, !71, !"_ZN5numba7cpython7unicode11_cmp_regionB3v35B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex12unicode_typexx: %retptr"}
!71 = distinct !{!71, !"_ZN5numba7cpython7unicode11_cmp_regionB3v35B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex12unicode_typexx"}
!72 = !{!"branch_weights", i32 99, i32 1}
!73 = !{!74, !76, !18, !20}
!74 = distinct !{!74, !75, !"_ZN5numba7cpython7unicode10unicode_eq12_3clocals_3e7eq_implB3v33B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type12unicode_type: %retptr"}
!75 = distinct !{!75, !"_ZN5numba7cpython7unicode10unicode_eq12_3clocals_3e7eq_implB3v33B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type12unicode_type"}
!76 = distinct !{!76, !75, !"_ZN5numba7cpython7unicode10unicode_eq12_3clocals_3e7eq_implB3v33B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type12unicode_type: %excinfo"}
!77 = !{!78, !80, !18, !20}
!78 = distinct !{!78, !79, !"_ZN5numba7cpython7unicode10unicode_eq12_3clocals_3e7eq_implB3v33B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type12unicode_type: %retptr"}
!79 = distinct !{!79, !"_ZN5numba7cpython7unicode10unicode_eq12_3clocals_3e7eq_implB3v33B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type12unicode_type"}
!80 = distinct !{!80, !79, !"_ZN5numba7cpython7unicode10unicode_eq12_3clocals_3e7eq_implB3v33B42c8tJTIcFHzwl2ILiXkcBV0KBSmNGHkyiCKJEEwA_3dE12unicode_type12unicode_type: %excinfo"}
!81 = !{!82, !26, !28, !84, !18, !20}
!82 = distinct !{!82, !83, !"_ZN5numba7cpython7unicode15_get_code_pointB3v36B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex: %retptr"}
!83 = distinct !{!83, !"_ZN5numba7cpython7unicode15_get_code_pointB3v36B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex"}
!84 = distinct !{!84, !85, !"_ZN5numba7cpython7unicode11_cmp_regionB3v35B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex12unicode_typexx: %retptr"}
!85 = distinct !{!85, !"_ZN5numba7cpython7unicode11_cmp_regionB3v35B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex12unicode_typexx"}
!86 = !{!87, !26, !28, !89, !18, !20}
!87 = distinct !{!87, !88, !"_ZN5numba7cpython7unicode15_get_code_pointB3v36B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex: %retptr"}
!88 = distinct !{!88, !"_ZN5numba7cpython7unicode15_get_code_pointB3v36B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex"}
!89 = distinct !{!89, !90, !"_ZN5numba7cpython7unicode11_cmp_regionB3v35B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex12unicode_typexx: %retptr"}
!90 = distinct !{!90, !"_ZN5numba7cpython7unicode11_cmp_regionB3v35B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex12unicode_typexx"}
!91 = !{!92}
!92 = distinct !{!92, !93, !"_ZN5numba7cpython7unicode15_get_code_pointB3v36B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex: %retptr"}
!93 = distinct !{!93, !"_ZN5numba7cpython7unicode15_get_code_pointB3v36B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex"}
!94 = !{!95}
!95 = distinct !{!95, !96, !"_ZN5numba7cpython7unicode15_get_code_pointB3v36B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex: %retptr"}
!96 = distinct !{!96, !"_ZN5numba7cpython7unicode15_get_code_pointB3v36B38c8tJTIcFHzwl2ILiXkcBV0KBSmNGHlhCEwA_3dE12unicode_typex"}
