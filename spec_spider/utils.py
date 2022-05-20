import re

import pandas as pd


def get_detail_url(o_url, suffix):
    """ Get detail url
    :param o_url: http://spec.org/cpu2017/results/cint2017.html
    :param suffix: res2018q1/cpu2017-20171224-02025.html
    :return: http://spec.org/cpu2017/results/res2018q1/cpu2017-20171224-02025.html
    """
    items = o_url.split('/')
    return '/'.join(items[:-1] + [suffix])


def delete_br(raw_html):
    return re.sub('\n', ' ', re.sub('<br>', '', raw_html))


def delete_tag(raw_html):
    cleaner = re.compile('<.*?>')
    return re.sub(cleaner, '', raw_html)


def delete_tag_and_br(raw_html):
    return delete_br(delete_tag(raw_html).strip()).strip()


def clean_vendor(vendor):
    def _clear(pattern):
        return re.sub(re.compile(pattern, re.IGNORECASE), '', vendor)

    vendor = vendor.strip()
    patterns = [
        ' *[(].*[)]',
        ',* *Ltd\.*$|,* *Inc\.*$',
        ',* *Co\.*$|,* *Corporation\.*$|,* *Corparation\.*$|,* *Corp\.*$| Incoporated$| Incorporated$| Incorporation$',
        ' International$',
        ' Computer[s]*$',
        ' Technology$',
    ]
    for pattern in patterns:
        vendor = _clear(pattern)

    replace_pairs = [
        ('^Huawei', 'Huawei'),
        ('^ASUS', 'ASUS'),
        ('^acer', 'Acer'),
        ('^Hewlett[ -]*Packard', 'HPE'),
        ('^Inspur', 'Inspur'),
        ('H3C', 'H3C'),
        ('^Giga[ -]*byte', 'Gigabyte'),
        ('^Fujitsu', 'Fujitsu'),
        ('^Hitachi', 'Hitachi'),
        ('^Lenovo', 'Lenovo'),
        ('^Quanta', 'Quanta'),
        ('^Super[ -]*Micro', 'SuperMicro'),
        ('^UNIWIDE', 'Uniwide'),
        ('^Wizbrain', 'Wizbrain'),
        ('^ScaleMP', 'ScaleMP'),
        ('^AMD', 'AMD'),
        ('Advanced Micro Devices', 'AMD'),
        ('^Hewelett-Packard', 'HPE'),
        ('^Oracl', 'Oracle'),
        ('^BEA', 'BEA'),
        ('^OpenJDK', 'OpenJDK'),
    ]
    for pair in replace_pairs:
        if len(re.findall(re.compile(pair[0], re.IGNORECASE), vendor)):
            vendor = pair[1]

    return vendor


def parse_system_name(info):
    info = re.sub('[(].*[)]', '', info)
    info = re.sub('[(]|[)]', '', info)
    info = info.strip()
    info = info.split(',')[0]
    info = re.sub(' AMD.*?', '', info)
    info = re.sub(' Intel.*', '', info)
    info = re.sub('\d+.*\d*GHz$', '', info)
    info = re.sub('^vSMP ServerONE Supermicro ', '', info)
    info = re.sub('^.*GHz ', '', info)
    info = re.sub('^.*Core ', '', info)
    info = info.strip()
    return info


def get_cpu_vendor(cpu_name):
    item = cpu_name.split()[0]
    if item in ['Intel', 'AMD', 'Huawei']:
        vendor = item
    else:
        vendor = 'Other'
    return vendor


def parse_cpu_orderable(info):
    info = info.split(';')[0]
    items = re.findall('.*chip', info)
    number = 1
    if len(items):
        nums = [int(num) for num in re.findall(r'\d+', items[0])]
        number = nums[-1]
    return number


def get_total_cores(cpu_info):
    items = cpu_info.split(',')
    total_cores = int(items[0].split()[0])
    return total_cores


def get_chips(cpu_info):
    items = cpu_info.split(',')
    chips = int(items[1].split()[0])
    return chips


def get_threads_per_core(cpu_info):
    items = cpu_info.split(',')
    threads_per_core = 1
    if len(items) == 3:
        threads_per_core = int(items[-1].split()[0])
    return threads_per_core


def get_total_memory_amount(memory_info):
    total_memory_amount = int(memory_info.split()[0])
    unit = memory_info.split()[1]
    if 'T' in unit:
        total_memory_amount *= 1024
    return total_memory_amount


def get_memory_number(memory_info):
    items = re.findall(r'\d+[ ]*x|\d+[ ]*\*', memory_info)
    nums = [int(re.sub('[ ]*x|[ ]*\*', '', item)) for item in items]
    # items = re.findall(r'\d+ x', memory_info)
    # nums = [int(item.split()[0]) for item in items]
    memory_num = sum(nums)
    if memory_num == 0:
        memory_num = 1
    return memory_num
    

def parse_storage(info):
    if 'SSD' in info.upper():
        storage_type = 'SSD'
    elif 'HDD' in info.upper():
        storage_type = 'HDD'
    elif 'ramfs' in info.lower():
        storage_type = 'ramfs'
    elif 'tmpfs' in info.lower():
        storage_type = 'tmpfs'
    elif 'zfs' in info.lower():
        storage_type = 'zfs'
    else:
        storage_type = 'SSD'

    return storage_type


def parse_cpu_char(info):
    items = re.findall('\d+\.*\d*[ ]?G[ ]?Hz', info)
    if len(items) == 0:
        items = re.findall('\d+\.*\d*[ ]?M[ ]?Hz', info)
        if len(items) == 0:
            value = 0
        else:
            value = round(float(re.sub('[ ]?M[ ]?Hz', '', items[-1])) / 1000, 2)
    else:
        value = round(float(re.sub('[ ]?G[ ]?Hz', '', items[-1])), 2)
    return value


month_mapper = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May':5, 'Jun': 6,
    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
}
re_month_mapper = {v:k for k, v in month_mapper.items()}


def clean_date_1(item):
    """
    May-2017
    2014.3
    2010
    """
    item = item.strip()
    d = 1
    if '-' in item:
        m, y = item.split('-')
    elif '.' in item:
        y, m = item.split('.')
        m = re_month_mapper[int(m)]
    elif ' ' in item:
        m, y = item.split()
    else:
        y = item
        m = 'Jan'
    m = month_mapper[m[:3]]
    y = int(y)
    if len(str(y)) == 2:
        y = int(f"20{str(y)}")
    return f"{y}-{m}-{d}"


def clean_date_2(item):
    """
    May 1, 2018
    2009/05/19
    12.02.2009
    """
    if ',' in item:
        md, y = item.split(',')
        m, d = md.split()
    elif '/' in item:
        y, m, d = item.split('/')
        m = re_month_mapper[int(m)]
    elif '.' in item:
        d, m, y = item.split('.')
        m = re_month_mapper[int(m)]
    m = m[:3]
    if m == 'Spe':
        m = 'Sep'
    m = month_mapper[m[:3]]
    d, m, y = int(d), int(m), int(y)
    if len(str(y)) == 2:
        y = int(f"20{str(y)}")
    return f"{y}-{m}-{d}"


def clean_date_3(item):
    """ Thu Aug 20 04:16:03 GMT-05:00 2015 """
    items = item.split()
    d, m, y = items[2], items[1], items[-1]
    m = m[:3]
    m = month_mapper[m]
    d, m, y = int(d), int(m), int(y)
    if len(str(y)) == 2:
        y = int(f"20{str(y)}")
    return f"{y}-{m}-{d}"


def get_submit_year(url):
    return int(url.split('/')[0][3:7])


def get_submit_quarter(url):
    return int(url.split('/')[0][-1])


def get_full_url(url):
    """
    res2022q1/cpu2017-20211123-30269.html
    """
    benchmark = url.split('/')[-1].split('-')[0]
    full_url = f"https://spec.org/{benchmark}/results/{url}"
    return full_url
