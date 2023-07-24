CREATE TABLE customer_courier_chat_messages (
  Sender_app_type VARCHAR(20),
  Custome_r_id INT,
  From_id INT,
  To_id INT,
  Chat_started_by_message BOOLEAN,
  Order_id INT,
  Order_stage VARCHAR(30),
  Courier_id INT,
  Message_sent_time DATETIME
  );

  INSERT INTO customer_courier_chat_messages (Sender_app_type, Custome_r_id, From_id, To_id, Chat_started_by_message, Order_id, Order_stage, Courier_id, Message_sent_time) VALUES ('Customer iOS', 17071099, 17071099, 16293039, FALSE, 59528555, 'PICKING_UP', 16293039, '2019-08-19 8:01:47');

    INSERT INTO customer_courier_chat_messages (Sender_app_type, Custome_r_id, From_id, To_id, Chat_started_by_message, Order_id, Order_stage, Courier_id, Message_sent_time) VALUES ('Courier iOS', 17071099, 16293039, 17071099, FALSE, 59528555, 'ARRIVING', 16293039, '2019-08-19 8:01:04');

  SELECT * FROM customer_courier_chat_messages;

  CREATE TABLE orders (
  	order_id INT,
    city_code VARCHAR(30)
  );

  INSERT INTO orders (order_id, city_code) VALUES (59528555, 64100);

  CREATE TABLE conversations (
  	order_id INT,
    city_code INT,
    first_courier_message DATETIME,
    first_customer_message DATETIME,
    num_messages_courier INT,
    num_messages_customer INT,
    first_message_by VARCHAR(15),
    conversation_started_at TIMESTAMP,
    first_responsetime_delay_seconds TIME,
    last_message_time DATETIME,
    last_message_order_stage VARCHAR(30)
  );

INSERT INTO conversations (
  order_id,
  city_code,
  first_courier_message,
  first_customer_message,
  num_messages_courier,
  num_messages_customer,
  first_message_by,
  conversation_started_at,
  first_responsetime_delay_seconds,
  last_message_time,
  last_message_order_stage
)
SELECT
  C.Order_id,
  O.city_code,
  MIN(CASE WHEN C.Sender_app_type LIKE 'Courier%' THEN C.Message_sent_time END),
  MIN(CASE WHEN C.Sender_app_type LIKE 'Customer%' THEN C.Message_sent_time END),
  SUM(CASE WHEN C.Sender_app_type LIKE 'Courier%' THEN 1 ELSE 0 END) AS num_messages_courier,
  SUM(CASE WHEN C.Sender_app_type LIKE 'Customer%' THEN 1 ELSE 0 END) AS num_messages_customer,
  CASE
    WHEN MIN(CASE WHEN C.Sender_app_type LIKE 'Courier%' THEN C.Message_sent_time END) <
         MIN(CASE WHEN C.Sender_app_type LIKE 'Customer%' THEN C.Message_sent_time END) THEN 'Courier'
    ELSE 'Customer'
  END,
  MIN(C.Message_sent_time),
  TIMESTAMPDIFF(SECOND,
    CASE
      WHEN MIN(CASE WHEN C.Sender_app_type LIKE 'Courier%' THEN C.Message_sent_time END) <
           MIN(CASE WHEN C.Sender_app_type LIKE 'Customer%' THEN C.Message_sent_time END) THEN MIN(CASE WHEN C.Sender_app_type LIKE 'Courier%' THEN C.Message_sent_time END)
      ELSE MIN(CASE WHEN C.Sender_app_type LIKE 'Customer%' THEN C.Message_sent_time END)
    END,
    MIN(C.Message_sent_time)
  ) AS first_responsetime_delay_seconds,
  MAX(C.Message_sent_time) AS last_message_time,
  MAX(C.Order_stage) AS last_message_order_stage
FROM customer_courier_chat_messages AS C
JOIN orders AS O
  ON C.Order_id = O.order_id
GROUP BY C.Order_id, O.city_code;

SELECT * FROM conversations;
