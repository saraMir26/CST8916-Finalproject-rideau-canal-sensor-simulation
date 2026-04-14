import json
import os
import random
import time
from datetime import datetime, timezone

from azure.iot.device import IoTHubDeviceClient, Message
from dotenv import load_dotenv


load_dotenv()

SEND_INTERVAL_SECONDS = int(os.getenv("SEND_INTERVAL_SECONDS", "10"))

DEVICE_CONFIGS = [
    {
        "deviceId": "dowslake-sensor",
        "location": "Dow's Lake",
        "connectionString": os.getenv("DOWSLAKE_DEVICE_CONNECTION_STRING"),
        "iceRange": (26, 36),
        "surfaceRange": (-8, 1),
        "snowRange": (0, 8),
        "externalRange": (-15, 2),
    },
    {
        "deviceId": "fifthavenue-sensor",
        "location": "Fifth Avenue",
        "connectionString": os.getenv("FIFTHAVENUE_DEVICE_CONNECTION_STRING"),
        "iceRange": (24, 34),
        "surfaceRange": (-7, 2),
        "snowRange": (0, 10),
        "externalRange": (-14, 3),
    },
    {
        "deviceId": "nac-sensor",
        "location": "NAC",
        "connectionString": os.getenv("NAC_DEVICE_CONNECTION_STRING"),
        "iceRange": (22, 32),
        "surfaceRange": (-6, 3),
        "snowRange": (0, 12),
        "externalRange": (-13, 4),
    },
]


def random_value(value_range):
    return round(random.uniform(value_range[0], value_range[1]), 2)


def build_sensor_message(device):
    return {
        "deviceId": device["deviceId"],
        "location": device["location"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "iceThickness": random_value(device["iceRange"]),
        "surfaceTemperature": random_value(device["surfaceRange"]),
        "snowAccumulation": random_value(device["snowRange"]),
        "externalTemperature": random_value(device["externalRange"]),
    }


def main():
    clients = []

    try:
        for device in DEVICE_CONFIGS:
            connection_string = device["connectionString"]
            if not connection_string:
                raise ValueError(f"Missing connection string for {device['deviceId']}")

            client = IoTHubDeviceClient.create_from_connection_string(connection_string)
            client.connect()
            clients.append((device, client))
            print(f"Connected: {device['deviceId']}")

        while True:
            for device, client in clients:
                payload = build_sensor_message(device)
                message_json = json.dumps(payload)
                message = Message(message_json)
                message.content_encoding = "utf-8"
                message.content_type = "application/json"

                client.send_message(message)
                print(f"Sent from {device['deviceId']}: {message_json}")

            time.sleep(SEND_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("Simulation stopped by user.")

    finally:
        for _, client in clients:
            try:
                client.disconnect()
            except Exception:
                pass


if __name__ == "__main__":
    main()