Entity Schema
=============

Properties
----------

-  **version**
-  **kind**
-  **schema** *(array)*

   -  **Items**

-  **name** *(string)*
-  **readable_storage** *(string)*
-  **writable_storage** *([‘string’, ‘null’])*
-  **query_processors** *(array)*

   -  **Items** *(object)*: Cannot contain additional properties.

      -  **processor** *(string)*
      -  **args** *(object)*

-  **translation_mappers** *(object)*: Cannot contain additional
   properties.

   -  **functions** *(array)*

      -  **Items** *(object)*: Cannot contain additional properties.

         -  **mapper** *(string)*
         -  **args** *(object)*

   -  **curried_functions** *(array)*

      -  **Items** *(object)*: Cannot contain additional properties.

         -  **mapper** *(string)*
         -  **args** *(object)*

   -  **subscriptables** *(array)*

      -  **Items** *(object)*: Cannot contain additional properties.

         -  **mapper** *(string)*
         -  **args** *(object)*

-  **validators** *(array)*

   -  **Items** *(object)*: Cannot contain additional properties.

      -  **validator** *(string)*
      -  **args** *(object)*

-  **required_time_column** *(string)*
