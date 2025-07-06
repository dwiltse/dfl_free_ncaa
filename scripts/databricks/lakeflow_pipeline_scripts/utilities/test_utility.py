"""
The 'utilities' folder contains Python modules.
Keeping them separate provides a clear overview
of utilities you can reuse across your transformations.
"""
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

@udf(returnType=FloatType())
def distance_km(distance_miles):
    """
    Converts distance in miles to kilometers.

    Example usage:

    import dlt
    from pyspark.sql.functions import col
    from utilities import new_utils

    @dlt.table
    def my_table():
        return (
            spark.read.table("samples.nyctaxi.trips")
            .withColumn("trip_distance_km", new_utils.distance_km(col("trip_distance")))
        )
    """
    # 1 mile = 1.60934 km
    return distance_miles * 1.60934