**Take Home Test - Data Engineer**

Please follow the guidelines below:

- You need to complete this test in a **maximum of 7 days** from the day you receive it.
- **Submit the results through Greenhouse in a ZIP file.**
- **Your deliverable must be self-contained**, so please include any files that are needed to run your process, use your notebooks, etc.

**What we value:**

- Following the instructions and doing everything from the checklist
- Clear, coherent and concise written communication
- Clean coding style
- Efficient solutions to the problems given
1. **KPIs (20 points)**



|**OUTPUT FORMAT**| ● A **PDF** file                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| - |--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|**WHAT TO DO [Checklist]**| <p>![](Aspose.Words.3279dbea-c87f-4444-a0fa-25fe0f24c5b3.001.png) In your opinion, **what are three important KPIs** for G**** and why?</p><p>***Note:** Ignore pure financial KPIs that apply to every business.*</p><p>Please **choose one** of these three KPIs and:</p><p>![](Aspose.Words.3279dbea-c87f-4444-a0fa-25fe0f24c5b3.002.png) Make an educated guess of its value and provide a step-by-step explanation of your guess.</p><p>![](Aspose.Words.3279dbea-c87f-4444-a0fa-25fe0f24c5b3.003.png) Suggest at least one idea that could significantly improve this KPI.</p> |
|**WHAT ARE WE LOOKING FOR?**| <p>- Good understanding of the business challenges we are facing</p><p>- Ability to **communicate and summarize**</p>                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
2. **SQL (40 points)**



<table><tr><th colspan="1" rowspan="2" valign="top"><b>OUTPUT FORMAT</b></th></tr>
<tr><td colspan="1" valign="top"><p>[DB Fiddle l](https://www.db-fiddle.com)ink that actually runs on <b>PostgreSQL 9.6</b> OR</p><p>[SQL Fiddle l](http://sqlfiddle.com/)ink that actually runs on <b>PostgreSQL 9.6</b>.</p></td></tr>
<tr><td colspan="1" valign="top"><b>WHAT TO DO [Checklist]</b></td><td colspan="1" valign="top"><p>- DDL for creating the SQL tables: customer_courier_chat_messages andorders</p><p>- Insert data in the SQL tables (you can use the one from the example below)</p><p>- Create the final table with a query</p><p>- [OPTIONAL] Include tests for the final table</p></td></tr>
<tr><td colspan="1"><b>WHAT ARE WE LOOKING FOR?</b></td><td colspan="1"><p>- Fundamental <b>SQL knowledge</b></p><p>- Ability to write clean, understandable and efficient queries</p></td></tr>
</table>

You have the ***customer\_courier\_chat\_messages*** table that stores data about individual messages exchanged between customers and couriers via the in-app chat. An example of the table is below:



|**Sender app type**|**Custome r id**|**From id**|**To id**|**Chat started by message**|**Order id**|**Order stage**|**Courier id**|**Message sent time**|
| :- | :- | - | - | :- | - | - | :- | - |
|Customer iOS|17071099|17071099|16293039|FALSE|59528555|PICKING\_UP|16293039|2019-08-19 8:01:47|
|Courier iOS|17071099|16293039|17071099|FALSE|59528555|ARRIVING|16293039|2019-08-19 8:01:04|
|Customer iOS|17071099|17071099|16293039|FALSE|59528555|PICKING\_UP|16293039|2019-08-19 8:00:04|
|Courier Android|12874122|18325287|12874122|TRUE|59528038|ADDRESS\_D ELIVERY|18325287|2019-08-19 7:59:33|

You also have access to the **orders** table where you have an order\_id and city\_code field.

Your task is to build a query that creates a table (including the DDL CREATE statement) (***customer\_courier\_conversations***) that aggregates individual messages into conversations. Take into consideration that a conversation is unique per order. The required fields are the following:

- **order\_id**
- **city\_code**
- **first\_courier\_message**: Timestamp of the first courier message
- **first\_customer\_message**: Timestamp of the first customer message
- **num\_messages\_courier**: Number of messages sent by courier
- **num\_messages\_customer**: Number of messages sent by customer
- **first\_message\_by**: The first message sender (courier or customer)
- **conversation\_started\_at**: Timestamp of the first message in the conversation
- **first\_responsetime\_delay\_seconds**: Time (in secs) elapsed until the first message was responded
- **last\_message\_time**: Timestamp of the last message sent
- **last\_message\_order\_stage**: The stage of the order when the last message was sent

Make your query scalable and readable!

3. **Events Processing (40 points)**



|**OUTPUT FORMAT**|<p>A ZIP file containing:</p><p>- The code in a **Python file / Jupyter notebook**</p><p>- The **input data**</p><p>- The **output data** as a **single CSV file**</p><p>- [OPTIONAL] Include the config files (like requirements.txt or pyproject.toml)</p>|
| - | - |
|**WHAT TO DO [Checklist]**|<p>- **Develop ETL code for creating** the output. We suggest you to use Pandas or Spark</p><p>- Declare **functions** with docstrings</p><p>- Include **comments** so that it’s easy to follow</p><p>- Write **clean code** following Python standards</p>|
|**WHAT ARE WE**|● Proficiency in **Python** and related data processing packages|



|**LOOKING FOR?**|<p>(Pandas/PySpark)</p><p>●</p><p>Following coding best practices</p>|
| - | - |

You have been given a dataset (*appEventProcessingDataset.tar.gz*) of three csv files containing the following:

1. Client HTTP endpoint polling event data for a set of devices running a web application.
1. Internet connectivity status logs for the above set of devices, generated when a device goes offline whilst running the application.
1. Orders data for orders that have been dispatched to devices running the above web application.

We’re interested in knowing about the connectivity environment of a device in the period of time surrounding when an order is dispatched to it.

Using Python and any useful libraries, ***produce a single csv formatted output dataset*** that contains the following information:

For each order dispatched to a device:

- The total count of all polling events
- The count of each type of polling status\_code
- The count of each type of polling error\_code and the count of responses without error codes.

...across the following periods of time:

- Three minutes before the order creation time
- Three minutes after the order creation time
- One hour before the order creation time

In addition to the above, across an unbounded period of time, we would like to know:

1. The time of the polling event immediately preceding, and immediately following the order creation time.
2. The most recent connectivity status (“ONLINE” or “OFFLINE”) before an order, and at what time the order changed to this status. This can be across any period of time before the order creation time. Not all devices have a connectivity status.

Please include all code used to produce your output, and any explanatory notes.
