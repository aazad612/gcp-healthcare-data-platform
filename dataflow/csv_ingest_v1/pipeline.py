def build_pipeline(p, runtime_metadata):

    dlq_methods = set(
        m.strip().lower()
        for m in runtime_metadata["dlq_method"].split(",")
    )

    # ---- Read + Validate (unchanged) ----
    rows = p | "ReadInputFile" >> read_csv(runtime_metadata)

    results = (
        rows
        | "ValidateAndParse" >> beam.ParDo(
            ValidateAndParseDoFn(runtime_metadata)
        ).with_outputs(
            TAG_SCHEMA,
            TAG_TYPE,
            TAG_PK,
            TAG_BQ,
            TAG_CRITICAL,
            main=TAG_VALID
        )
    )

    valid_rows = results[TAG_VALID]

    # ---- PK hash ----
    valid_rows = (
        valid_rows
        | "ComputePKHash" >> beam.Map(
            compute_pk_hash,
            pk_columns=runtime_metadata["pk_columns"]
        )
    )

    write_good_rows(valid_rows, runtime_metadata)

    # ---- Normalize DLQ records ----
    def normalize(tag):
        return beam.Map(
            build_dlq_record,
            runtime_metadata=runtime_metadata,
            error_category=tag
        )

    schema_dlq = results[TAG_SCHEMA] | "SchemaDLQ" >> normalize("SCHEMA")
    type_dlq   = results[TAG_TYPE]   | "TypeDLQ"   >> normalize("TYPE")
    pk_dlq     = results[TAG_PK]     | "PKDLQ"     >> normalize("PK")
    bq_dlq     = results[TAG_BQ]     | "BQDLQ"     >> normalize("BQ")
    critical   = results[TAG_CRITICAL] | "CriticalDLQ" >> normalize("CRITICAL")

    # ---- Conditional fanout (THIS is the key change) ----
    if "bq" in dlq_methods:
        write_bq_dlq(schema_dlq, runtime_metadata)
        write_bq_dlq(type_dlq, runtime_metadata)
        write_bq_dlq(pk_dlq, runtime_metadata)

    if "gcs" in dlq_methods:
        write_gcs_dlq(bq_dlq, runtime_metadata)
