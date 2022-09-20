Readable Storage Schema
=======================

Properties
----------

-  **version**
-  **kind**
-  **name** *(string)*
-  **storage** *(object)*: Cannot contain additional properties.

   -  **key** *(string)*
   -  **set_key** *(string)*

-  **schema** *(object)*: Cannot contain additional properties.

   -  **columns** *(array)*

      -  **Items**

   -  **local_table_name** *(string)*
   -  **dist_table_name** *(string)*

-  **query_processors** *(array)*

   -  **Items** *(string)*
