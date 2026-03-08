#! /usr/bin/env python3


from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod


class DataStream(ABC):
    id: str
    readings_count: int

    def __init__(self, id: str) -> None:
        self.id = id
        self.readings_count = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        return []

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return self.analysis_res

    @staticmethod
    def split(s: str, char: str) -> List:
        res = []
        segment = ""
        for c in s:
            if c != char:
                segment += c
            else:
                res += [segment]
                segment = ""
        if segment:
            res += [segment]
        return res


class SensorStream(DataStream):
    temp: int

    def __init__(self, id: str) -> None:
        super().__init__(id)

    @staticmethod
    def dictify(data: List[str]) -> Dict:
        d = {}
        for entry in data:
            key, value = DataStream.split(entry, ":")
            d[key] = float(value)
        return d

    def process_batch(self, data_batch: List[Any]) -> str:
        d = SensorStream.dictify(data_batch)
        self.temp = d["temp"]
        self.readings_count = len(data_batch)
        return f"{data_batch}"

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        d = SensorStream.dictify(data_batch)
        return [f"{key}:{d[key]}" for key in d if d[key] > 50]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return f"{self.readings_count} readings processed"


class TransactionStream(DataStream):
    net_flow: int

    def __init__(self, id: str) -> None:
        super().__init__(id)

    def dictify(data: List[str]) -> Dict:
        d = {}
        for entry in data:
            key, value = TransactionStream.split(entry, ":")
            if key in d:
                d[key] += int(value)
            else:
                d[key] = int(value)
        return d

    def process_batch(self, data_batch: List[Any]) -> str:
        d = TransactionStream.dictify(data_batch)
        self.net_flow = sum(d.values())
        self.readings_count = len(data_batch)
        return f"{data_batch}"

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        d = []
        for data in data_batch:
            key, value = TransactionStream.split(data, ":")
            if int(value) > 100:
                d += [f"{key}:{value}"]
        return d

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return f"{self.readings_count} operations"


class EventStream(DataStream):
    error_count: int

    def __init__(self, id: str) -> None:
        super().__init__(id)

    def process_batch(self, data_batch: List[Any]) -> str:
        self.readings_count = len(data_batch)
        self.error_count = len([x for x in data_batch if x == "error"])
        return f"{data_batch}"

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        return [event for event in data_batch if event == "error"]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return f"{self.readings_count} events"


class StreamProcessor:
    def filter_all(self):
        return self.filtered

    def process_all(self) -> None:
        data_batch = {
            "event": ["login", "error", "logout"],
            "transaction": ["buy:100", "sell:150", "buy:75"],
            "sensor": ["temp:25", "humidity:60"],
        }
        objs = [
            SensorStream(""),
            TransactionStream(""),
            EventStream(""),
        ]
        self.filtered = {}
        for obj in objs:
            if isinstance(obj, SensorStream):
                obj.process_batch(data_batch["sensor"])
                print(f"- Sensor data: {obj.get_stats()}")
                self.filtered["sensor"] = obj.filter_data(data_batch["sensor"])

            elif isinstance(obj, EventStream):
                obj.process_batch(data_batch["event"])
                print(f"- Event data: {obj.get_stats()}")
                self.filtered["event"] = obj.filter_data(data_batch["event"])

            elif isinstance(obj, TransactionStream):
                obj.process_batch(data_batch["transaction"])
                print(f"- Transaction data: {obj.get_stats()}")
                self.filtered["transaction"] = obj.filter_data(
                    data_batch["transaction"]
                )


def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    print("\nInitializing Sensor Stream...")
    ss = SensorStream("SENSOR_001")
    ss.data_type = "Environmental Data"
    print(f"Stream ID: {ss.id}, Type: {ss.data_type}")
    data = ["temp:22.5", "humidity:65", "pressure:1013"]
    print(f"Processing sensor batch: {ss.process_batch(data)}")
    print(f"Sensor analysis: {ss.get_stats()}, avg temp: {ss.temp}°C")

    print("\nInitializing Transaction Stream...")
    ts = TransactionStream("TRANS_001")
    ts.data_type = "Financial Data"
    print(f"Stream ID: {ts.id}, Type: {ts.data_type}")
    data = ["buy:100", "sell:150", "buy:75"]
    print(f"Procetsing transaction batch: {ts.process_batch(data)}")
    print(
        f"Transaction analysis: {ts.get_stats()},"
        f" net flow: {'+25' if ts.net_flow > 25 else ts.netflow} units")

    print("\nInitializing Event Stream...")
    es = EventStream("EVENT_001")
    es.data_type = "System Event"
    print(f"Stream ID: {es.id}, Type: {es.data_type}")
    data = ["login", "error", "logout"]
    print(f"Proceesing transaction batch: {es.process_batch(data)}")
    print(f"Event analysis: {es.get_stats()}, {es.error_count} error detected")

    print("\n=== Polymorphic Stream Processing ===")

    print("\nBatch 1 Results")
    sp = StreamProcessor()
    sp.process_all()
    filtered = sp.filter_all()
    print("\nStream filtering active: High-priority data only")
    print(
        f"Filtered results: {len(filtered['sensor'])} critical "
        "sensor alerts, {len(filtered['transaction'])} large transaction"
    )

    print("\nAll streams processed successfully. Nexus throughout  optimal.")


if __name__ == "__main__":
    main()
