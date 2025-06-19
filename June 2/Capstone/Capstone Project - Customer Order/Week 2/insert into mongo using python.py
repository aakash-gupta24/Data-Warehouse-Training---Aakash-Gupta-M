from pymongo import MongoClient
from datetime import datetime

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["customerOrders"]
collection = db["customer_feedback"]

# Feedback data
feedback_data = [
    {"customer_id": 1, "order_id": 1, "feedback_text": "The headphones were delivered on time, very satisfied.", "submitted_at": datetime.fromisoformat('2025-05-05T10:00:00')},
    {"customer_id": 1, "order_id": 15, "feedback_text": "Great delivery experience for the wireless charger.", "submitted_at": datetime.fromisoformat('2025-05-10T14:30:00')},
    {"customer_id": 2, "order_id": 24, "feedback_text": "Laptop stand arrived early and in perfect condition.", "submitted_at": datetime.fromisoformat('2025-05-14T09:15:00')},
    {"customer_id": 3, "order_id": 2, "feedback_text": "Delivery of the USB-C hub was smooth and quick.", "submitted_at": datetime.fromisoformat('2025-05-21T16:45:00')},
    {"customer_id": 4, "order_id": 39, "feedback_text": "Very happy with the delivery service for the mechanical keyboard.", "submitted_at": datetime.fromisoformat('2025-05-22T11:00:00')},
    {"customer_id": 5, "order_id": 3, "feedback_text": "Webcam was handled with care.", "submitted_at": datetime.fromisoformat('2025-05-29T11:20:00')},
    {"customer_id": 6, "order_id": 10, "feedback_text": "Timely delivery of the gaming mouse and good packaging.", "submitted_at": datetime.fromisoformat('2025-05-30T13:00:00')},
    {"customer_id": 7, "order_id": 4, "feedback_text": "Monitor was delivered as promised, excellent service.", "submitted_at": datetime.fromisoformat('2025-06-01T12:10:00')},
    {"customer_id": 8, "order_id": 38, "feedback_text": "Bluetooth speaker arrived safely and on time.", "submitted_at": datetime.fromisoformat('2025-06-02T15:30:00')},
    {"customer_id": 9, "order_id": 32, "feedback_text": "Keyboard delivery was punctual and hassle-free.", "submitted_at": datetime.fromisoformat('2025-06-03T10:25:00')},
    {"customer_id": 10, "order_id": 5, "feedback_text": "Excellent delivery experience with the smart watch.", "submitted_at": datetime.fromisoformat('2025-06-06T09:00:00')},
    {"customer_id": 11, "order_id": 30, "feedback_text": "Tablet arrived in perfect condition.", "submitted_at": datetime.fromisoformat('2025-06-07T14:45:00')},
    {"customer_id": 12, "order_id": 28, "feedback_text": "Delivery of the fitness tracker exceeded expectations.", "submitted_at": datetime.fromisoformat('2025-06-08T10:15:00')},
    {"customer_id": 13, "order_id": 50, "feedback_text": "Great communication throughout the webcam delivery.", "submitted_at": datetime.fromisoformat('2025-06-09T12:00:00')},
    {"customer_id": 14, "order_id": 21, "feedback_text": "Portable SSD arrived without damage.", "submitted_at": datetime.fromisoformat('2025-06-10T11:10:00')},
    {"customer_id": 15, "order_id": 8, "feedback_text": "Very satisfied with the power bank delivery process.", "submitted_at": datetime.fromisoformat('2025-06-11T13:35:00')},
    {"customer_id": 16, "order_id": 33, "feedback_text": "Gaming headset delivered on schedule, thank you.", "submitted_at": datetime.fromisoformat('2025-06-12T14:50:00')},
    {"customer_id": 17, "order_id": 31, "feedback_text": "Prompt delivery of smart LED bulb and courteous service.", "submitted_at": datetime.fromisoformat('2025-06-13T09:30:00')},
    {"customer_id": 18, "order_id": 17, "feedback_text": "Wireless earbuds arrived earlier than expected.", "submitted_at": datetime.fromisoformat('2025-06-14T10:20:00')},
    {"customer_id": 19, "order_id": 45, "feedback_text": "Smooth delivery experience with the digital photo frame.", "submitted_at": datetime.fromisoformat('2025-06-15T12:40:00')},
    {"customer_id": 20, "order_id": 7, "feedback_text": "Gaming controller delivered carefully and on time.", "submitted_at": datetime.fromisoformat('2025-06-16T15:10:00')},
    {"customer_id": 21, "order_id": 11, "feedback_text": "Very happy with the service for the HDMI cable.", "submitted_at": datetime.fromisoformat('2025-06-17T11:55:00')},
    {"customer_id": 22, "order_id": 12, "feedback_text": "Delivery was efficient and timely for the portable fan.", "submitted_at": datetime.fromisoformat('2025-06-18T09:45:00')},
    {"customer_id": 23, "order_id": 13, "feedback_text": "Excellent handling of the desk lamp package.", "submitted_at": datetime.fromisoformat('2025-06-19T14:05:00')},
    {"customer_id": 24, "order_id": 25, "feedback_text": "Package with phone holder arrived in perfect condition.", "submitted_at": datetime.fromisoformat('2025-06-20T10:30:00')},
    {"customer_id": 25, "order_id": 14, "feedback_text": "Great delivery of the webcam mount, will order again.", "submitted_at": datetime.fromisoformat('2025-06-21T12:25:00')},
    {"customer_id": 26, "order_id": 26, "feedback_text": "Satisfied with quick delivery of the laptop bag.", "submitted_at": datetime.fromisoformat('2025-06-22T13:50:00')},
    {"customer_id": 27, "order_id": 24, "feedback_text": "Delivery went smoothly without issues for the desk organizer.", "submitted_at": datetime.fromisoformat('2025-06-23T14:10:00')},
    {"customer_id": 28, "order_id": 29, "feedback_text": "Package with phone charger delivered safely and on time.", "submitted_at": datetime.fromisoformat('2025-06-24T15:20:00')},
    {"customer_id": 29, "order_id": 27, "feedback_text": "Excellent customer service during delivery of the stylus pen.", "submitted_at": datetime.fromisoformat('2025-06-25T11:40:00')},
    {"customer_id": 30, "order_id": 16, "feedback_text": "Fast delivery of the power adapter, very pleased.", "submitted_at": datetime.fromisoformat('2025-06-26T10:50:00')},
    {"customer_id": 31, "order_id": 19, "feedback_text": "Delivered with care and on schedule for the laptop charger.", "submitted_at": datetime.fromisoformat('2025-06-27T12:15:00')},
    {"customer_id": 32, "order_id": 44, "feedback_text": "Order for ethernet cable arrived intact and timely.", "submitted_at": datetime.fromisoformat('2025-06-28T14:00:00')},
    {"customer_id": 33, "order_id": 18, "feedback_text": "Very good delivery experience with the phone tripod.", "submitted_at": datetime.fromisoformat('2025-06-29T09:35:00')},
    {"customer_id": 34, "order_id": 28, "feedback_text": "Package with smart scale arrived as expected.", "submitted_at": datetime.fromisoformat('2025-06-30T10:45:00')},
    {"customer_id": 35, "order_id": 35, "feedback_text": "Timely and professional delivery of the wireless keyboard.", "submitted_at": datetime.fromisoformat('2025-07-01T11:20:00')},
    {"customer_id": 36, "order_id": 49, "feedback_text": "Order for webcam lens cover was handled very well.", "submitted_at": datetime.fromisoformat('2025-07-02T13:10:00')},
    {"customer_id": 37, "order_id": 41, "feedback_text": "Package of mouse pad delivered without delays.", "submitted_at": datetime.fromisoformat('2025-07-03T14:30:00')},
    {"customer_id": 38, "order_id": 36, "feedback_text": "Quick delivery of surge protector, very happy.", "submitted_at": datetime.fromisoformat('2025-07-04T12:40:00')},
    {"customer_id": 39, "order_id": 42, "feedback_text": "Laptop cooling pad arrived on schedule.", "submitted_at": datetime.fromisoformat('2025-07-05T15:00:00')},
    {"customer_id": 40, "order_id": 43, "feedback_text": "Very pleased with the delivery of the screen protector.", "submitted_at": datetime.fromisoformat('2025-07-06T11:55:00')},
    {"customer_id": 41, "order_id": 37, "feedback_text": "Delivery was quick and hassle-free for the HDMI splitter.", "submitted_at": datetime.fromisoformat('2025-07-07T13:25:00')},
    {"customer_id": 42, "order_id": 34, "feedback_text": "Order for USB cable arrived safely and on time.", "submitted_at": datetime.fromisoformat('2025-07-08T14:50:00')},
    {"customer_id": 43, "order_id": 6, "feedback_text": "Excellent delivery service for the tablet case.", "submitted_at": datetime.fromisoformat('2025-07-09T12:10:00')},
    {"customer_id": 44, "order_id": 22, "feedback_text": "Package was handled with great care – phone cover arrived safe.", "submitted_at": datetime.fromisoformat('2025-07-10T10:30:00')},
    {"customer_id": 45, "order_id": 23, "feedback_text": "Delivery of the phone holder went very smoothly.", "submitted_at": datetime.fromisoformat('2025-07-11T15:40:00')}
]

# Insert data
collection.insert_many(feedback_data)

# Create index
collection.create_index([("customer_id", 1)])

print("Data inserted and index created.")
