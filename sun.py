
# COMMAND ----------
# MAGIC %run cadp-global-scm-analytics/notebooks/utils/render_utils
# COMMAND ----------
dbutils.widgets.dropdown(name="yaml_env", defaultValue="acon_render", choices=["acon_render", "dev", "prod"])
env = dbutils.widgets.get("yaml_env")
# COMMAND ----------
spark.sql("""SET spark.databricks.delta.optimizeWrite.enabled=True;""")
spark.sql("""SET spark.databricks.delta.autoCompact.enabled=True;""")
spark.sql("""set spark.sql.legacy.timeParserPolicy=LEGACY;""")

# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StringType,ArrayType,IntegerType,DateType
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import *