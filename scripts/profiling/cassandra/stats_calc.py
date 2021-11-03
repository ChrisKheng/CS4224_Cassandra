import argparse
import csv
import math
import os
import pathlib
import re

class Client:
    def __init__(self, no="", a="", b="", c="", d="", e="", f="", g=""):
        self.client_no = no
        self.num = a
        self.total_transaction_time = b
        self.throughput = c
        self.average_latency = d
        self.median_latency = e
        self.ninety_five = f
        self.ninety_nine = g
        self.dic = {
            "New Order" : [],
            "Related Customer" : [],
            "Payment" : [],
            "Popular Item" : [],
            "Top Balance" : [],
            "Stock Level" : [],
            "Delivery" : [],
            "Order Status" : [],
        }
        
    def to_csv_line(self):
        return f"{self.client_no},{self.num},{self.total_transaction_time},{self.throughput},{self.average_latency},{self.median_latency},{self.ninety_five},{self.ninety_nine}"

    def to_client_line(self):
        out = f"{self.client_no}\n"
        for k in self.dic:
            out += ",".join(self.dic[k]) + "\n"
        return out

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Compute statistics for Wholesale')
    parser.add_argument("--i", help="Input path", default=".")
    parser.add_argument("--o", help="Output path", default=".")
    args = parser.parse_args()

    ipath = args.i
    opath = args.o

    clients = dict()
    for i in pathlib.Path(ipath).rglob("*.err"):
        with open(i, "r") as f:
            n = i.stem

            lines = [_.strip() for _ in f]
            if not lines[0]:
                lines.pop(0)

            try:
                a = re.findall("\d+", lines[2])[0]
                b = re.findall("\d+", lines[3])[0]
                c = re.findall("\d+", lines[4])[0]
                d = re.findall("\d+", lines[5])[0]
                e = re.findall("\d+", lines[6])[0]
                f = re.findall("\d+", lines[7])[1]
                g = re.findall("\d+", lines[8])[1]
                client = Client(n,a,b,c,d,e,f,g)
            except:
                client = Client(n)

            for i in range(11, len(lines), 8):
                xact_name = lines[i]
                for offset in range(1,6):
                    client.dic[xact_name].append(re.findall("\d+", lines[i+offset])[0])
                client.dic[xact_name].append(re.findall("\d+", lines[i+6])[1])
                client.dic[xact_name].append(re.findall("\d+", lines[i+7])[1]) 

            clients[int(n)] = client


    nc = len(clients)
    avg = 0
    min_throughput = math.inf
    max_throughput = -math.inf

    with open(f"{opath}/clients.csv", 'w') as clientsf:
        for i in range(40):

            if i in clients:
                clientsf.write(clients[i].to_csv_line())
                clientsf.write("\n")

                throughput = int(clients[i].throughput)

                avg += throughput/nc
                min_throughput = min([min_throughput, throughput])
                max_throughput = max([max_throughput, throughput])

                with open(f"{opath}/{i}.txt", 'w') as cf:
                    cf.write(clients[i].to_client_line())

    with open(f"{opath}/throughput.csv", 'w') as tf:
        tf.write(f"{min_throughput},{max_throughput},{avg}")
