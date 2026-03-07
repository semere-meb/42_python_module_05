#! /usr/bin/env python3


from typing import Any, List
from abc import ABC, abstractmethod


class BaseProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return result


class NumericProcessor(BaseProcessor):
    def process(self, data: Any) -> str:
        try:
            sum = 0
            count = 0
            for val in data:
                sum += val
                count += 1
            avg = sum / count
        except ZeroDivisionError:
            print("ERROR: no items in the data")
            count = 0
            avg = 0
            sum = 0
        finally:
            result = f"Processed {count} numeric values, sum={sum}, avg={avg}"
        return result

    def validate(self, data: Any) -> bool:
        for val in data:
            if val.__class__ not in (int, float):
                return False
        else:
            return True


class TextProcessor(BaseProcessor):
    @staticmethod
    def count_chars(s: str) -> int:
        char_count = 0
        for char in s:
            char_count += 1
        return char_count

    @staticmethod
    def count_words(s: str) -> int:
        i = 0
        word_count = 0
        char_count = TextProcessor.count_chars(s)
        while i < char_count:
            while i < char_count and s[i] in (" ", "\t", "\n", "\r"):
                i += 1
            start = i
            while i < char_count and s[i] not in (" ", "\t", "\n", "\r"):
                i += 1
            if i - start:
                word_count += 1
        return word_count

    def process(self, data: Any) -> str:
        try:
            char_count = TextProcessor.count_chars(data)
            word_count = TextProcessor.count_words(data)
        except ValueError:
            char_count = 0
            word_count = 0
        finally:
            result = f"Processed  text: {char_count} characters, {word_count} words"
        return result

    def validate(self, data: Any) -> bool:
        return data.__class__ is str


class LogProcessor(BaseProcessor):
    logs = ["ERROR", "WARN", "INFO"]

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

    def process(self, data: Any) -> str:
        if self.validate(data):
            status, msg = LogProcessor.split(data, ":")
        else:
            status = "INVALID"
            msg = "INVALID MESSAGE"
        result = f"[{status}] {status} level detected: {msg}"
        return result

    def validate(self, data: Any) -> bool:
        count = 0
        for i in LogProcessor.split(data, ":"):
            count += 1
        if count > 2:
            return False
        status, msg = LogProcessor.split(data, ":")
        return status in self.logs


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")

    print("\nInitializing Numeric Processor...")
    np = NumericProcessor()
    data = [1, 2, 3, 4, 5]
    print(f"Processing data: {data}")
    process_res = np.process(data)
    print(
        "Validation: numeric data " + f"{'' if np.validate(data) else 'not '}verified"
    )
    print(f"Output: {np.format_output(process_res)}")

    print("\nInitializing Text Processor...")
    tp = TextProcessor()
    data = "Hello Nexus world"
    print(f'Processing data: "{data}"')
    process_res = tp.process(data)
    print("Validation: Text data " + f"{'' if tp.validate(data) else 'not '}verified")
    print(f"Output: {tp.format_output(process_res)}")

    print("\nInitializing Log Processor...")
    lp = LogProcessor()
    data = "ERROR: Connection timeout"
    print(f'Processing data: "{data}"')
    process_res = lp.process(data)
    print("Validation: Log entry " + f"{'' if lp.validate(data) else 'not '}verified")
    print(f"Output: {lp.format_output(process_res)}")

    print("\n=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through the same interface")
    res = [np, tp, lp]
    data = [[1, 2, 3], "Hello python world!", "INFO:Using python"]
    for i, v in enumerate(res):
        print(f"Result {i}: ", res[i].process(data[i]))

    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
