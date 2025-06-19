from pymongo import MongoClient
from datetime import datetime

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["supplyChainMonitoring"]
shipment_logs = db["shipment_logs"]

# Insert multiple shipment documents
shipment_data = [
    {"shipment_id": 1, "order_id": 7, "shipment_date": datetime(2025, 6, 8), "carrier": "FedEx", "status": "delivered", "tracking_number": "FDX123456789", "quantity_shipped": 25},
    {"shipment_id": 2, "order_id": 3, "shipment_date": datetime(2025, 6, 4), "carrier": "UPS", "status": "in transit", "tracking_number": "UPS987654321", "quantity_shipped": 15},
    {"shipment_id": 3, "order_id": 10, "shipment_date": datetime(2025, 6, 12), "carrier": "DHL", "status": "delayed", "tracking_number": "DHL2468101214", "quantity_shipped": 30},
    {"shipment_id": 4, "order_id": 25, "shipment_date": datetime(2025, 6, 26), "carrier": "FedEx", "status": "delivered", "tracking_number": "FDX135791113", "quantity_shipped": 25},
    {"shipment_id": 5, "order_id": 17, "shipment_date": datetime(2025, 7, 1), "carrier": "UPS", "status": "in transit", "tracking_number": "UPS112233445", "quantity_shipped": 11},
    {"shipment_id": 6, "order_id": 1, "shipment_date": datetime(2025, 6, 15), "carrier": "DHL", "status": "delivered", "tracking_number": "DHL667788990", "quantity_shipped": 10},
    {"shipment_id": 7, "order_id": 19, "shipment_date": datetime(2025, 7, 3), "carrier": "FedEx", "status": "in transit", "tracking_number": "FDX998877665", "quantity_shipped": 20},
    {"shipment_id": 8, "order_id": 23, "shipment_date": datetime(2025, 7, 5), "carrier": "UPS", "status": "delivered", "tracking_number": "UPS554433221", "quantity_shipped": 17},
    {"shipment_id": 9, "order_id": 12, "shipment_date": datetime(2025, 7, 7), "carrier": "DHL", "status": "delivered", "tracking_number": "DHL123443211", "quantity_shipped": 14},
    {"shipment_id": 10, "order_id": 8, "shipment_date": datetime(2025, 7, 9), "carrier": "FedEx", "status": "delayed", "tracking_number": "FDX554466778", "quantity_shipped": 17},
    {"shipment_id": 11, "order_id": 14, "shipment_date": datetime(2025, 7, 10), "carrier": "UPS", "status": "delivered", "tracking_number": "UPS667788990", "quantity_shipped": 21},
    {"shipment_id": 12, "order_id": 20, "shipment_date": datetime(2025, 7, 12), "carrier": "DHL", "status": "in transit", "tracking_number": "DHL998877665", "quantity_shipped": 10},
    {"shipment_id": 13, "order_id": 16, "shipment_date": datetime(2025, 7, 14), "carrier": "FedEx", "status": "delivered", "tracking_number": "FDX112244668", "quantity_shipped": 19},
    {"shipment_id": 14, "order_id": 21, "shipment_date": datetime(2025, 7, 15), "carrier": "UPS", "status": "delivered", "tracking_number": "UPS556677889", "quantity_shipped": 18},
    {"shipment_id": 15, "order_id": 5, "shipment_date": datetime(2025, 7, 16), "carrier": "DHL", "status": "in transit", "tracking_number": "DHL112233445", "quantity_shipped": 14},
    {"shipment_id": 16, "order_id": 9, "shipment_date": datetime(2025, 7, 18), "carrier": "FedEx", "status": "delivered", "tracking_number": "FDX556644332", "quantity_shipped": 9},
    {"shipment_id": 17, "order_id": 6, "shipment_date": datetime(2025, 7, 19), "carrier": "UPS", "status": "cancelled", "tracking_number": "UPS000000000", "quantity_shipped": 5},
    {"shipment_id": 18, "order_id": 18, "shipment_date": datetime(2025, 7, 21), "carrier": "DHL", "status": "delivered", "tracking_number": "DHL889977665", "quantity_shipped": 22},
    {"shipment_id": 19, "order_id": 4, "shipment_date": datetime(2025, 7, 22), "carrier": "FedEx", "status": "in transit", "tracking_number": "FDX445566778", "quantity_shipped": 12},
    {"shipment_id": 20, "order_id": 2, "shipment_date": datetime(2025, 7, 23), "carrier": "UPS", "status": "delivered", "tracking_number": "UPS998877665", "quantity_shipped": 20},
    {"shipment_id": 21, "order_id": 22, "shipment_date": datetime(2025, 7, 24), "carrier": "DHL", "status": "delayed", "tracking_number": "DHL334455667", "quantity_shipped": 6},
    {"shipment_id": 22, "order_id": 24, "shipment_date": datetime(2025, 7, 25), "carrier": "FedEx", "status": "in transit", "tracking_number": "FDX778899001", "quantity_shipped": 12},
    {"shipment_id": 23, "order_id": 11, "shipment_date": datetime(2025, 7, 26), "carrier": "UPS", "status": "delivered", "tracking_number": "UPS112211221", "quantity_shipped": 14},
    {"shipment_id": 24, "order_id": 15, "shipment_date": datetime(2025, 7, 27), "carrier": "DHL", "status": "delivered", "tracking_number": "DHL556622334", "quantity_shipped": 13},
    {"shipment_id": 25, "order_id": 26, "shipment_date": datetime(2025, 7, 28), "carrier": "FedEx", "status": "cancelled", "tracking_number": "FDX000000000", "quantity_shipped": 14},
    {"shipment_id": 26, "order_id": 27, "shipment_date": datetime(2025, 7, 29), "carrier": "UPS", "status": "in transit", "tracking_number": "UPS778899001", "quantity_shipped": 16},
    {"shipment_id": 27, "order_id": 28, "shipment_date": datetime(2025, 7, 30), "carrier": "DHL", "status": "delivered", "tracking_number": "DHL889900112", "quantity_shipped": 8},
    {"shipment_id": 28, "order_id": 13, "shipment_date": datetime(2025, 7, 31), "carrier": "FedEx", "status": "delivered", "tracking_number": "FDX223344556", "quantity_shipped": 16},
    {"shipment_id": 29, "order_id": 30, "shipment_date": datetime(2025, 8, 1), "carrier": "UPS", "status": "in transit", "tracking_number": "UPS667788990", "quantity_shipped": 21},
    {"shipment_id": 30, "order_id": 29, "shipment_date": datetime(2025, 8, 2), "carrier": "DHL", "status": "delivered", "tracking_number": "DHL998877665", "quantity_shipped": 23},
    {"shipment_id": 31, "order_id": 19, "shipment_date": datetime(2025, 8, 3), "carrier": "FedEx", "status": "in transit", "tracking_number": "FDX556677889", "quantity_shipped": 20},
    {"shipment_id": 32, "order_id": 10, "shipment_date": datetime(2025, 8, 4), "carrier": "UPS", "status": "delivered", "tracking_number": "UPS112233445", "quantity_shipped": 18},
    {"shipment_id": 33, "order_id": 16, "shipment_date": datetime(2025, 8, 5), "carrier": "DHL", "status": "delivered", "tracking_number": "DHL223344556", "quantity_shipped": 19},
    {"shipment_id": 34, "order_id": 8, "shipment_date": datetime(2025, 8, 6), "carrier": "FedEx", "status": "in transit", "tracking_number": "FDX334455667", "quantity_shipped": 12},
    {"shipment_id": 35, "order_id": 24, "shipment_date": datetime(2025, 8, 7), "carrier": "UPS", "status": "delivered", "tracking_number": "UPS445566778", "quantity_shipped": 12},
    {"shipment_id": 36, "order_id": 22, "shipment_date": datetime(2025, 8, 8), "carrier": "DHL", "status": "delivered", "tracking_number": "DHL556677889", "quantity_shipped": 14},
    {"shipment_id": 37, "order_id": 18, "shipment_date": datetime(2025, 8, 9), "carrier": "FedEx", "status": "cancelled", "tracking_number": "FDX000000001", "quantity_shipped": 22},
    {"shipment_id": 38, "order_id": 11, "shipment_date": datetime(2025, 8, 10), "carrier": "UPS", "status": "in transit", "tracking_number": "UPS998877665", "quantity_shipped": 14},
    {"shipment_id": 39, "order_id": 20, "shipment_date": datetime(2025, 8, 11), "carrier": "DHL", "status": "delivered", "tracking_number": "DHL112244668", "quantity_shipped": 12},
    {"shipment_id": 40, "order_id": 3, "shipment_date": datetime(2025, 8, 12), "carrier": "FedEx", "status": "delivered", "tracking_number": "FDX223355779", "quantity_shipped": 15},
]

result = shipment_logs.insert_many(shipment_data)
print(f"Inserted {len(result.inserted_ids)} shipment documents.")
