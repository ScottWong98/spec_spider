import re


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
