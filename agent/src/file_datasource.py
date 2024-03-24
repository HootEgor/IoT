from csv import reader
from datetime import datetime
from domain.aggregated_data import AggregatedData
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.parking import Parking
class FileDatasource:
    def __init__(self, accelerometer_filename: str, gps_filename: str, parking_filename: str) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename

        self.accelerometer_csv_reader = None
        self.gps_csv_reader = None
        self.parking_csv_reader = None

        self.accelerometer_column_names = None
        self.gps_column_names = None
        self.parking_column_names = None

        self.accelerometer_file = None
        self.gps_file = None
        self.parking_file = None

    def read(self) -> AggregatedData:
        accelerometer_data = self.read_accelerometer_data()
        gps_data = self.read_gps_data()
        parking_data = self.read_parking_data()

        time = datetime.now()

        return AggregatedData(
            accelerometer=Accelerometer(accelerometer_data[0], accelerometer_data[1], accelerometer_data[2]),
            gps=Gps(gps_data[0], gps_data[1]),
            parking=Parking(parking_data[0], Gps(parking_data[1], parking_data[2])),
            time=time
        )

    def read_accelerometer_data(self):
        try:
            data = next(self.accelerometer_csv_reader)
            while len(data) != 3:
                data = next(self.accelerometer_csv_reader)
            return data
        except StopIteration:
            self.accelerometer_file.close()
            self.accelerometer_file = open(self.accelerometer_filename, 'r')
            self.accelerometer_csv_reader = reader(self.accelerometer_file)
            self.accelerometer_column_names = next(self.accelerometer_csv_reader)
            return self.read_accelerometer_data()

    def read_gps_data(self):
        try:
            data = next(self.gps_csv_reader)
            while len(data) != 2:
                data = next(self.gps_csv_reader)
            return data
        except StopIteration:
            self.gps_file.close()
            self.gps_file = open(self.gps_filename, 'r')
            self.gps_csv_reader = reader(self.gps_file)
            self.gps_column_names = next(self.gps_csv_reader)
            return self.read_gps_data()

    def read_parking_data(self):
        try:
            data = next(self.parking_csv_reader)
            while len(data) != 3:
                data = next(self.parking_csv_reader)
            return data
        except StopIteration:
            self.parking_file.close()
            self.parking_file = open(self.parking_filename, 'r')
            self.parking_csv_reader = reader(self.parking_file)
            self.parking_column_names = next(self.parking_csv_reader)
            return self.read_parking_data()

    def startReading(self, *args, **kwargs):
        self.accelerometer_file = open(self.accelerometer_filename, 'r')
        self.gps_file = open(self.gps_filename, 'r')
        self.parking_file = open(self.parking_filename, 'r')

        self.accelerometer_csv_reader = reader(self.accelerometer_file)
        self.gps_csv_reader = reader(self.gps_file)
        self.parking_csv_reader = reader(self.parking_file)

        self.accelerometer_column_names = next(self.accelerometer_csv_reader)
        self.gps_column_names = next(self.gps_csv_reader)
        self.parking_column_names = next(self.parking_csv_reader)

    def stopReading(self, *args, **kwargs):
        if self.accelerometer_file:
            self.accelerometer_file.close()
        if self.gps_file:
            self.gps_file.close()
        if self.parking_file:
            self.parking_file.close()