The Polars DataFrame Library
============================

Polars is a blazingly fast DataFrame library implemented in Rust, using the Apache Arrow Columnar Format as its memory model. It is a powerful tool for data manipulation and analysis.

I got interested in Polars after listening to an episode of the `Real Python Podcast <https://realpython.com/podcasts/rpp/193/>`_, where `Wes McKinney <https://wesmckinney.com/>`_, the creator of Pandas, talks about Polars and its performance and design benefits over Pandas! 

Why is Polars so amazing?
-------------------------

* **Performance**: It is extremely fast, often outperforming other popular DataFrame libraries like pandas, especially on larger datasets. This is due to its Rust backend, query optimization, and support for parallel execution.
* **Lazy Evaluation**: Polars can operate in a lazy mode, which means it builds up a query plan and only executes it when needed. This allows for significant performance optimizations.
* **Expressive API**: The API is intuitive and allows for writing clean and readable code for complex data transformations.
* **Memory Efficiency**: It's designed to be memory efficient.

If you are working with data in Python, especially larger than memory datasets, you should definitely give Polars a try. 

Helpful Usage Examples
----------------------

Here are some examples of Polars in action.

Reading Data
~~~~~~~~~~~~

You can easily read data from various sources like CSV, Parquet, and more.

.. code-block:: python

   import polars as pl

   # Read a CSV file
   df = pl.read_csv("your_data.csv")
   # or Lazy-load a CSV file
   df = pl.scan_csv("your_data.csv")

   # Create a DataFrame from a dictionary
   data = {'col1': [1, 2, 3], 'col2': ['A', 'B', 'C']}
   df_from_dict = pl.DataFrame(data)
   # or for example, a list of dictionaries
   data = [{'col1': 1, 'col2': 'A'}, {'col1': 2, 'col2': 'B'}, {'col1': 3, 'col2': 'C'}]
   df_from_list_of_dicts = pl.from_dicts(data)

Selecting and Filtering Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Polars has an expressive way to select and filter data.

.. code-block:: python

   # Select columns
   df.select("col1", "col2")

   # Regex-based column selection
   df.select(pl.col("^col.[1-9]$"))

   # Filter rows based on a condition
   df.filter(pl.col("col1") > 1)
   # or alternatively
   df.filter(pl.col("col1").gt(1))

Chaining operations in Polars is a breeze.

.. code-block:: python

   df.filter(pl.col("col1") > 1).select("col1", "col2")


Groupby and Aggregations
~~~~~~~~~~~~~~~~~~~~~~~~

Performing groupby operations and aggregations is straightforward.

.. code-block:: python

   # Group by a column and aggregate
   df.group_by("col2").agg(
       pl.sum("col1").alias("sum_col1"),
       pl.count().alias("count")
   )

Using the Lazy API
~~~~~~~~~~~~~~~~~~

For larger datasets, the lazy API can provide significant performance benefits.

.. code-block:: python

   # Use the lazy API to build a query plan
   q = (
       pl.scan_csv("your_data.csv")
       .filter(pl.col("col1") > 2)
       .group_by("col2")
       .agg(pl.sum("col1"))
   )

   # Execute the query
   result = q.collect()

Detecting Local Minima
~~~~~~~~~~~~~~~~~~~~~~

Here is a more advanced example that I faced in one of the projects I worked on. It demonstrates the power of Polars' expression chaining to find the number of local minima in a series within groups.

A local minimum is a point where the value is lower than its immediate neighbors. This can be found by looking for a change in the sign of the difference of the series from negative to positive. Assume the DataFrame is structured with a ``case_idx`` column to identify different cases, and an ``y`` column that holds the values where we'll detect minima. Also, assume that the DataFrame is sorted by ``case_idx`` and ``x`` already.

.. code-block:: python

   df = pl.DataFrame(
       {
           "case_idx": [1, 1, 1, 2, 2, 2],
           "y": [1, 2, 3, 4, 5, 6],
       }
   )
   # Example of finding local minima in column "e_l" for each "case_idx"
   loc_min_df = df.group_by("case_idx", maintain_order=True).agg(
           pl.col("y")
           .diff()
           .sign()
           .replace(0, None)
           .fill_null(strategy="forward")
           .diff()
           .eq(2)
           .sum()
           .alias("n_local_min_e")
   )

   print(loc_min_df)
   # Output:
   # ┌──────────┬───────────────┐
   # │ case_idx ┆ n_local_min_e │
   # │ ---      ┆ ---           │
   # │ i64      ┆ u32           │
   # ╞══════════╪═══════════════╡
   # │ 1        ┆ 1             │
   # │ 2        ┆ 0             │
   # └──────────┴───────────────┘
   

This chain of operations does the following:

1.  ``pl.col("y")`` selects the ``y`` column.
2.  ``diff()`` calculates the diff of the ``y`` column to see the change between consecutive values.
3.  ``sign()`` gets the direction of change (-1 for decrease, 1 for increase, 0 for no change).
4.  Plateaus (no change) are removed with ``replace(0, None)`` and then filled with the previous direction using ``fill_null(strategy="forward")``.
5.  A second ``diff()`` on the signs will result in ``2`` where the sign changed from -1 to 1 (a valley, or local minimum).
6.  ``eq(2).sum()`` counts how many times this occurs for each group.
7.  ``alias("n_local_min_e")`` renames the output column to ``n_local_min_e``.

This is a concise and highly efficient way to perform what would be a much more complex operation in other libraries. 