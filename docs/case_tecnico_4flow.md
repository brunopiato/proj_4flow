You work as a data engineer for a parcel service. Your company sends parcels to pick-up stations exclusively. There is an increasing number of complaints from final recipients about late or missing deliveries recently. The root causes are unclear. The head of your department wants you to set up a tight monitoring to get an idea about hotspots.
You have access to the following data:

- Delivery Events: Messages that are created by the parcel vehicle driver whenever they deliver of a parcel at a pickup station. It comprises:
	- Date and time of delivery
	- Identifier of the pickup station
	- Name and address of the final recipient
	- Identifier of the driver
	- Parcel ID
	- Purchase Order ID

- Complaints: Messages that contain the complaint of the final recipient
	- Date and time of complaint
	- Identifier of the pickup station
	- Name and address of the final recipient
	- Purchase Order ID (optional)
	- Parcel ID (optional)
	- List of Pickup Locations:
	- Identifier
	- Name
	- City
	- Zip Code
	- Street

- Delivery events and complaints arrive in your environment as files on a central file server via different channels and individual data formats (json, xml, csv). Unfortunately, <u>**the identifiers of the pickup stations are not consolidated between the files for complaints and the files for delivery events**</u>. The <u>**list of pickup locations is contained in a database**</u> that is maintained by another team. You have reading access. You know that pickup locations are added, renamed or removed occasionally

- You are asked to do everything needed to create a <u>**daily report**</u> showing the following:
	- Identifier, Name and Zip Code of Pickup locations affected by claims together with the according number of claims within the last 14 calendar days
	- Name and address of final recipients with more than 5 complaints in the last two months and the given pickup locations of the according complaints per recipient
- You have access to a relational database system for the job
- Based on the information provided, please perform the following tasks:
- Create a suitable relational database schema. Write down the SQL statements for the according tables
- Write queries for the specified reports
- Design a data ingestion pipeline to get the incoming data into your database schema. Detail the ETL schema based on a Cloud Service solution of your choice and justify your choices

- Note: if you not have the information you need, make the appropriate assumptions