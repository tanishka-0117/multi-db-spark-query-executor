def read_query(spark, config, query):
    """
    Read data from any JDBC database using Spark.
    Config format matches databases.json.
    """

    reader = (
        spark.read
        .format("jdbc")
        .option("url", config["url"])
        .option("query", query)
        .option("user", config["user"])
        .option("password", config["password"])
        .option("driver", config["driver"])
    )

    # Optional partition settings for parallel JDBC reads
    if "partitionColumn" in config:
        reader = (
            reader
            .option("partitionColumn", config["partitionColumn"])
            .option("lowerBound", config["lowerBound"])
            .option("upperBound", config["upperBound"])
            .option("numPartitions", config["numPartitions"])
        )

    return reader.load()