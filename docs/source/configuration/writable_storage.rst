Writable Storage Schema
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

-  **stream_loader** *(object)*: Cannot contain additional properties.

   -  **processor** *(string)*
   -  **default_topic** *(string)*
   -  **commit_log_topic** *([‘string’, ‘null’])*
   -  **subscription_scheduled_topic** *([‘string’, ‘null’])*
   -  **subscription_scheduler_mode** *([‘string’, ‘null’])*
   -  **subscription_result_topic** *([‘string’, ‘null’])*
   -  **replacement_topic** *([‘string’, ‘null’])*
   -  **pre_filter** *(object)*: Cannot contain additional properties.

      -  **type** *(string)*
      -  **args** *(array)*

         -  **Items** *(string)*

   -  **dlq_policy** *(object)*: Cannot contain additional properties.

      -  **type** *(string)*
      -  **args** *(array)*

         -  **Items** *(string)*
