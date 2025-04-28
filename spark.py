from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Инициализация SparkSession
spark = SparkSession.builder.master("local").appName("ProductCategoryExample").getOrCreate()

# Примерные данные для продуктов и категорий
products_data = [(1, "Product A"), (2, "Product B"), (3, "Product C"), (4, "Product D")]
categories_data = [(1, "Category X"), (2, "Category Y")]
product_categories_data = [(1, 1), (2, 1), (3, 2)]

# Создание датафреймов
products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
product_categories_df = spark.createDataFrame(product_categories_data, ["product_id", "category_id"])

# 1. Пары «Имя продукта – Имя категории»
product_category_pairs = (
    products_df
    .join(product_categories_df, "product_id")
    .join(categories_df, "category_id")
    .select("product_name", "category_name")
)

# 2. Продукты, которые не имеют категории
products_without_category = (
    products_df
    .join(product_categories_df, "product_id", "left_anti")  # Используем left_anti для фильтрации без категории
    .select("product_name")
)

# Показать результаты
product_category_pairs.show()
products_without_category.show()
