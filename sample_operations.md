Sample test cases for CRUD operations on a customer database:

{"operation": "read", "fields": ["*"], "where": {"name": "dhurandhar"}}             -> This is for query sample : say that even without customer_id it works.

{"customer_id": 99999,"name": "rehman dakait","email": "uzair_baloch"}         -> This is for INSERT sample : It demonstrates rigorous CRUD operation

{"customer_id": 123456,"name": "rehman dakait","email": "uzair_baloch"}     -> Then show that Query works with customer_id 123445

{"name": "rehman dakait"} ->  UPDATE : IT WILL FAIL.
{"customer_id": 123456} -> now it will work because we have customer_id in the where clause.

{"customer_id": 123456} -> After this query and show that it does not exist/ dakait got deleted.

