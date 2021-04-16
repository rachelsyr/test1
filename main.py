import time
from concurrent.futures import ThreadPoolExecutor
import os

domains = {}
clients = {}
IPs = {}

domain_with_label = []
with open("whiteFeaturesAll.txt", "r") as f:
    for line in f.readlines():
        line = line.removesuffix('\n')
        key = line.split('\t')[0]
        domain_with_label.append(key)
with open("blackFeaturesAll.txt", "r") as f:
    for line in f.readlines():
        line = line[:-1]
        key = line.removesuffix('\n')
        domain_with_label.append(key)

def process(string):
    print(f"start process {string}")
    global domain_with_label
    domains_IPs = {}
    IPs_domains = {}
    domains_clients = {}
    clients_domains = {}
    with open(string, "r") as f:
        for line in f.readlines():
            if line[13] == '' or line[13] not in domain_with_label:
                continue
            line = line.removesuffix('\n')
            words = line.split('\t')
            # print(words)
            # 将字符串形式的时间转换为时间元组
            # 将时间元组转换为时间戳
            _t = time.mktime(time.strptime(words[0], "%Y-%m-%d %H:%M:%S"))
            # print(_t)
            _IPsrc = words[1]  # jiexi
            _IPdest = words[2]  # qingqiuzhe
            _DNS = words[13]
            if _DNS not in domains_IPs.keys():
                domains_IPs[_DNS] = []
                domains_clients[_DNS] = []
            if _IPsrc not in IPs_domains.keys():
                IPs_domains[_IPsrc] = []
            if _IPdest not in clients_domains.keys():
                clients_domains[_IPdest] = []
            if _IPsrc not in domains_IPs[_DNS]:
                domains_IPs[_DNS].append(_IPsrc)
            if _IPdest not in domains_clients[_DNS]:
                domains_clients[_DNS].append(_IPdest)
            if _DNS not in clients_domains[_IPdest]:
                clients_domains[_IPdest].append(_DNS)
            if _DNS not in IPs_domains[_IPsrc]:
                IPs_domains[_IPsrc].append(_DNS)
        print(f"Finish {string}.")
    return domains_IPs, IPs_domains, domains_clients, clients_domains

def batch_process(path):
    pool = ThreadPoolExecutor(max_workers=150)
    futures = []
    domains_IPs = {}
    IPs_domains = {}
    domains_clients = {}
    clients_domains = {}
    for fpathe, dirs, fs in os.walk(path):
        for f in fs:
            future = pool.submit(process, os.path.join(fpathe, f))
            futures.append(future)
    for future in futures:
        domains_IPs_tmp, IPs_domains_tmp, domains_clients_tmp, clients_domains_tmp = future.result()
        print(domains_clients_tmp)
        for key in domains_IPs_tmp:
            if key not in domains_IPs.keys():
                domains_IPs[key] = []
            domains_IPs[key].extend(domains_IPs_tmp[key])
        for key in IPs_domains_tmp:
            if key not in IPs_domains.keys():
                IPs_domains[key] = []
            IPs_domains[key].extend(IPs_domains_tmp[key])
        for key in domains_clients_tmp:
            if key not in domains_clients.keys():
                domains_clients[key] = []
            domains_clients[key].extend(domains_clients_tmp[key])
        for key in clients_domains_tmp:
            if key not in clients_domains.keys():
                clients_domains[key] = []
            clients_domains[key].extend(clients_domains_tmp[key])
    for key in domains_IPs.keys():
        domains_IPs[key] = list(set(domains_IPs[key]))
    for key in domains_IPs.keys():
        IPs_domains[key] = list(set(IPs_domains[key]))
    for key in domains_clients.keys():
        domains_clients[key] = list(set(domains_clients[key]))
    for key in clients_domains.keys():
        clients_domains[key] = list(set(clients_domains[key]))
    pool.shutdown()
    return domains_IPs, IPs_domains, domains_clients, clients_domains


# domains_IPs = {}
# IPs_domains = {}
# domains_clients = {}
# clients_domains = {}
# with open("resp_20170519_03_site115.log", "r") as f:
#     for line in f.readlines():
#         if line[13] == '':
#             continue
#         line = line.removesuffix('\n')
#         words = line.split('\t')
#         # print(words)
#         # 将字符串形式的时间转换为时间元组
#         # 将时间元组转换为时间戳
#         _t = time.mktime(time.strptime(words[0], "%Y-%m-%d %H:%M:%S"))
#         # print(_t)
#         _IPsrc = words[1]  # jiexi
#         _IPdest = words[2]  # qingqiuzhe
#         _DNS = words[13]
#         if _DNS not in domains_IPs.keys():
#             domains_IPs[_DNS] = []
#             domains_clients[_DNS] = []
#         if _IPsrc not in IPs_domains.keys():
#             IPs_domains[_IPsrc] = []
#         if _IPdest not in clients_domains.keys():
#             clients_domains[_IPdest] = []
#         if _IPsrc not in domains_IPs[_DNS]:
#             domains_IPs[_DNS].append(_IPsrc)
#         if _IPdest not in domains_clients[_DNS]:
#             domains_clients[_DNS].append(_IPdest)
#         if _DNS not in clients_domains[_IPdest]:
#             clients_domains[_IPdest].append(_DNS)
#         if _DNS not in IPs_domains[_IPsrc]:
#             IPs_domains[_IPsrc].append(_DNS)
# print("domains_IPs")
# print(domains_IPs)
# print("IPs_domains")
# print(IPs_domains)
# print("domains_clients")
# print(domains_clients)
# print("clients_domains")
# print(clients_domains)

t = time.time()
res = batch_process('./database')
print(time.time()-t)
print("domains_IPs")
print(res[0])
print("IPs_domains")
print(res[1])
print("domains_clients")
print(res[2])
print("clients_domains")
print(res[3])