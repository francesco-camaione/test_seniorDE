<a name="br1"></a> 

**Take Home Test - Data Engineer**

Please follow the guidelines below:

●

**●**

**●**

You need to complete this test in a **maximum of 7 days** from the day you receive it.

**Submit the results through Greenhouse in a ZIP ﬁle.**

**Your deliverable must be self-contained**, so please include any ﬁles that are

needed to run your process, use your notebooks, etc.

**What we value:**

●

●

●

●

Following the instructions and doing everything from the checklist

Clear, coherent and concise written communication

Clean coding style

Efﬁcient solutions to the problems given

**1. KPIs (20 points)**

**OUTPUT FORMAT**

●

A **PDF** ﬁle

**WHAT TO DO**

**[Checklist]**

In your opinion, **what are three important KPIs** for Glovo and

why?

***Note:** Ignore pure ﬁnancial KPIs that apply to every business.*

Please **choose one** of these three KPIs and:

Make an educated guess of its value and provide a step-by-step

explanation of your guess.

Suggest at least one idea that could signiﬁcantly improve this

KPI.

**WHAT ARE WE**

**LOOKING FOR?**

●

●

Good understanding of the business challenges we are facing

Ability to **communicate and summarize**



<a name="br2"></a> 

**2. SQL (40 points)**

**OUTPUT FORMAT [DB](https://www.db-fiddle.com)[ ](https://www.db-fiddle.com)[Fiddle](https://www.db-fiddle.com)[ ](https://www.db-fiddle.com)**link that actually runs on **PostgreSQL 9.6**

OR

[SQL](http://sqlfiddle.com/)[ ](http://sqlfiddle.com/)[Fiddle](http://sqlfiddle.com/)[ ](http://sqlfiddle.com/)link that actually runs on **PostgreSQL 9.6**.

**WHAT TO DO**

**[Checklist]**

DDL for creating the SQL tables:

customer\_courier\_chat\_messages and orders

Insert data in the SQL tables (you can use the one from the

example below)

Create the ﬁnal table with a query

[OPTIONAL] Include tests for the ﬁnal table

**WHAT ARE WE**

**LOOKING FOR?**

●

●

Fundamental **SQL knowledge**

Ability to write clean, understandable and efﬁcient queries

You have the ***customer\_courier\_chat\_messages*** table that stores data about individual

messages exchanged between customers and couriers via the in-app chat. An example of the

table is below:

**Sender**

**Custome**

**r id**

**From id**

**To id**

**Chat**

**started**

**by**

**Order id**

**Order**

**stage**

**Courier**

**id**

**Message**

**sent time**

**app type**

**message**

Customer

iOS

17071099

17071099

16293039

FALSE

59528555

PICKING\_UP 16293039

2019-08-19

8:01:47

Courier

iOS

17071099

17071099

16293039

17071099

17071099

16293039

FALSE

FALSE

59528555

59528555

ARRIVING

16293039

2019-08-19

8:01:04

Customer

iOS

PICKING\_UP 16293039

2019-08-19

8:00:04

Courier

Android

12874122

18325287

12874122

TRUE

59528038

ADDRESS\_D

ELIVERY

18325287

2019-08-19

7:59:33

You also have access to the **orders** table where you have an order\_id and city\_code ﬁeld.



<a name="br3"></a> 

Your task is to build a query that creates a table (including the DDL CREATE statement)

(***customer\_courier\_conversations***) that aggregates individual messages into conversations.

Take into consideration that a conversation is unique per order. The required ﬁelds are the

following:

**● order\_id**

**● city\_code**

● **ﬁrst\_courier\_message**: Timestamp of the ﬁrst courier message

● **ﬁrst\_customer\_message**: Timestamp of the ﬁrst customer message

● **num\_messages\_courier**: Number of messages sent by courier

● **num\_messages\_customer**: Number of messages sent by customer

● **ﬁrst\_message\_by**: The ﬁrst message sender (courier or customer)

● **conversation\_started\_at**: Timestamp of the ﬁrst message in the conversation

● **ﬁrst\_responsetime\_delay\_seconds**: Time (in secs) elapsed until the ﬁrst message was

responded

● **last\_message\_time**: Timestamp of the last message sent

● **last\_message\_order\_stage**: The stage of the order when the last message was sent

Make your query scalable and readable!

**3. Events Processing (40 points)**

**OUTPUT FORMAT** A ZIP ﬁle containing:

●

●

●

●

The code in a **Python ﬁle / Jupyter notebook**

The **input data**

The **output data** as a **single CSV ﬁle**

[OPTIONAL] Include the conﬁg ﬁles (like requirements.txt or

pyproject.toml)

**WHAT TO DO**

**[Checklist]**

**Develop ETL code for creating** the output. We suggest you to

use Pandas or Spark

Declare **functions** with docstrings

Include **comments** so that it’s easy to follow

Write **clean code** following Python standards

**WHAT ARE WE**

●

Proﬁciency in **Python** and related data processing packages



<a name="br4"></a> 

**LOOKING FOR?**

(Pandas/PySpark)

Following coding best practices

●

You have been given a dataset (*appEventProcessingDataset.tar.gz*) of three csv ﬁles

containing the following:

1\. Client HTTP endpoint polling event data for a set of devices running a web

application.

2\. Internet connectivity status logs for the above set of devices, generated when a device

goes ofﬂine whilst running the application.

3\. Orders data for orders that have been dispatched to devices running the above web

application.

We’re interested in knowing about the connectivity environment of a device in the period of

time surrounding when an order is dispatched to it.

Using Python and any useful libraries, ***produce a single csv formatted output dataset*** that

contains the following information:

For each order dispatched to a device:

● The total count of all polling events

● The count of each type of polling status\_code

● The count of each type of polling error\_code and the count of responses without error

codes.

...across the following periods of time:

● Three minutes before the order creation time

● Three minutes after the order creation time

● One hour before the order creation time

In addition to the above, across an unbounded period of time, we would like to know:

1\. The time of the polling event immediately preceding, and immediately following the

order creation time.



<a name="br5"></a> 

2\. The most recent connectivity status (“ONLINE” or “OFFLINE”) before an order, and at

what time the order changed to this status. This can be across any period of time

before the order creation time. Not all devices have a connectivity status.

Please include all code used to produce your output, and any explanatory notes.

