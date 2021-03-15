import pandas as pd

filename = "string_data"
hostname = 'DFW-SV7-CER-DSW2_'
with open(filename, 'r') as f:
    config = f.read()
prefix = hostname
lines = config.split("\n")
names = []
ips = []
for line in lines:
    items = line.split(" ")
    items = list(filter(None, items))
    item = {"name": prefix + items[0], "ip": items[1]}
    names.append(prefix + items[0])
    ips.append(items[1])

pd_data = pd.DataFrame(
    {
        "NAME": names,
        "IP": ips,
    },
    columns=["NAME", "IP"]
)

pd_data.to_excel('saved_excel.xlsx')
