# -*- coding: utf-8 -*-

import csv
import urllib
import urllib2
import hashlib
import random
import string
import datetime

from StringIO import StringIO
from urlparse import urljoin
from xml.etree import ElementTree as ET


class NoBillExistException(Exception):
    """无账单时抛出的异常"""
    pass


class WeixinPayClient(object):
    """微信支付客户端

    实现基本的请求方法、签名方法
    """

    API_BASE_URL = "https://api.mch.weixin.qq.com"

    def __init__(self, **kwargs):
        """构造函数

        :param kwargs: Weixin Pay configuration dictionary, should contains below keys:
                       `appid`, `mch_id`, `sub_mch_id`, `key`
        """
        self._appid = kwargs["appid"]
        self._mch_id = kwargs["mch_id"]
        self._sub_mch_id = kwargs["sub_mch_id"]
        self._key = kwargs["key"]

    @staticmethod
    def _gen_nonce():
        return ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in xrange(16))

    @staticmethod
    def _to_xml_str(para):
        """将dict类型的para转换为xml字符串"""

        root = ET.Element("xml")
        for key, value in para.items():
            child = ET.Element(key)
            child.text = value
            root.append(child)
        return ET.tostring(root, encoding="utf-8", method="xml")

    @staticmethod
    def _parse_xml_str(xml_str):
        root = ET.fromstring(xml_str)
        xml_dict = {}
        for node in root.iterchildren():
            xml_dict[node.tag] = node.text
        return xml_dict

    def _get_para_sign(self, para):
        """获取参数签名"""

        sorted_para = "%s&key=%s" % ("&".join(["=".join(i) for i in sorted(para.items())]), self._key)
        signed_str = hashlib.md5(sorted_para).hexdigest()
        return signed_str.upper()

    def _request(self, method, url, para):
        """向目标url请求数据

        :param method: GET, POST
        :param url: 请求路径，可绝对或相对
        :param para: 未签名的请求参数
        :return: 响应正文
        """
        url = urljoin(self.API_BASE_URL, url)
        if not url:
            raise ValueError("Request url should be set")
        if not method or not isinstance(method, str) or method.upper() not in ("GET", "POST"):
            raise ValueError('Request method should be "GET" or "POST"')

        para["sign"] = self._get_para_sign(para)

        if method == "GET":
            req = urllib2.Request("%s?%s" % (url, urllib.urlencode(self.data)))
        else:
            req = urllib2.Request(url, self._to_xml_str(para))
        f = urllib2.urlopen(req)
        response_content = f.read()
        return response_content

    def _get(self, url, para):
        """向目标url请求数据（GET）"""

        return self._request("get", url, para)

    def _post(self, url, para):
        """向目标url请求数据（POST）"""

        return self._request("post", url, para)


class WeixinPayBillClient(WeixinPayClient):
    """获取账单"""

    _url = "/pay/downloadbill"

    def __init__(self, *args, **kwargs):
        """构造函数

        date: 字符串，获取账单日期
        bill_type: 字符串，可选：ALL, SUCCESS, REFUND，默认: ALL
        """
        super(type(self), self).__init__(*args, **kwargs)
        if kwargs.get("date", None):
            self.date = kwargs["date"]
        else:
            yesterday = datetime.date.today()-datetime.timedelta(days=1)
            self.date = yesterday.strftime("%Y%m%d")

        self.bill_type = kwargs.get("bill_type", "ALL")

        self._table_header = []

    def get_bill(self, date_string, nobillexception=False):
        """获取账单

        当nobillexception为True时，账单不存在时会抛出NoBillExistException,
        否则返回空字符串

        :param date_string: 要获取账单字符串, 格式: %Y%m%d
        :param nobillexception: 是否抛出NoBillExistException异常
        :return: 返回账单正文，无账单会返回空字符串
        """
        datetime.datetime.strptime(date_string, "%Y%m%d")
        para = {
            "appid": self._appid,
            "mch_id": self._mch_id,
            "nonce_str": self._gen_nonce(),
            "bill_date": date_string,
            "bill_type": self.bill_type,
        }
        if self._sub_mch_id:
            para["sub_mch_id"] = self._sub_mch_id
        content = self._post(self._url, para)
        if content.find("<xml>") != -1:
            for node in ET.fromstring(content).getchildren():
                if node.tag == 'return_msg':
                    msg = node.text
                    if msg == 'No Bill Exist':
                        if nobillexception:
                            raise NoBillExistException()
                        else:
                            return ""
                    break
            else:
                msg = u'未知错误'
            raise Exception(msg)
        else:
            return content

    def save_bill(self, date_string, file_path):
        """保存账单

        :param date_string: 要获取账单字符串, 格式: %Y%m%d
        :return: 无
        """
        with open(file_path, "wb") as f:
            f.write(self.get_bill(date_string))

    def get_trade_iterator(self, date_string):
        """获取交易迭代器

        迭代器值为数组, 说明如下
        索引  内容                  例子
        00   交易时间               `2014-11-19 19:44:40
        01   公众账号ID
        02   商户号                 `10010588
        03   子商户号               `0
        04   设备号                 `
        05   微信订单号             `1007440988201411190006028672
        06   商户订单号             `267963
        07   用户标识               `085e9858eca3bf372fc033504
        08   交易类型               `JSAPI
        09   交易状态               `SUCCESS
        10   付款银行               `CMB_CREDIT
        11   货币种类               `CNY
        12   总金额                 `0.01
        13   企业红包金额           `0.0
        14   微信退款单号           `0
        15   商户退款单号           `0
        16   退款金额               `0
        17   企业红包退款金额       `0
        18   退款类型               `
        19   退款状态               `
        20   商品名称               `出借 参与感 一个星期
        21   商户数据包             `
        22   手续费                 `0.00006
        23   费率                   `0.60%


        :param date_string: 要获取账单字符串, 格式: %Y%m%d
        :return: 交易iterator
        """
        bill_content = self.get_bill(date_string)
        if bill_content:
            temp_file = StringIO(bill_content)
            reader = csv.reader(temp_file)

            for index, row in enumerate(reader):
                if index == 0:
                    if not row[0].startswith("20"):
                        self._table_header = row
                        continue
                if len(row) == 24:
                    row = [value[1:] for value in row]
                    yield row
                else:
                    return


if __name__ == "__main__":
    WXPAY_CONF = {
        "appid": "",
        "mch_id": "",
        "sub_mch_id": "",
        "key": ""
    }

    date = datetime.date.today()
    for i in range(2):
        d = date-datetime.timedelta(i+1)
        bill_client = WeixinPayBillClient(**WXPAY_CONF)
        count = 0
        for i in bill_client.get_trade_iterator(d.strftime("%Y%m%d")):
            print i
            count += 1
        print count
