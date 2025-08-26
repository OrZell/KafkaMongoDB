import json

class Reader:

    def __init__(self):
        self.interesting_path = r'C:\Users\Or\PycharmProjects\KafkaMongoDB\data\newsgroups_interesting.json'
        self.not_interesting_path = r'C:\Users\Or\PycharmProjects\KafkaMongoDB\data\newsgroups_not_interesting.json'
        self.current_line = 0
        self.current_hop = 0

    def load_data_from_file(self, path, size=10):
        with open(path, mode="r", encoding='utf-8') as file:
            data = json.load(file)
        self.current_hop = size
        return data[self.current_line:self.current_line+size+1]

    def read_interesting_data(self):
        data = self.load_data_from_file(path=self.interesting_path)
        return data

    def read_not_interesting_data(self):
        data = self.load_data_from_file(self.not_interesting_path)
        return data

    def increase_the_counter(self):
        self.current_line += self.current_hop