# 读取csv文件，生成Cypher语言并写入图数据库
# -*- coding: utf-8 -*-
import csv
from py2neo import Graph, Node


class MAIN:
    def __init__(self):
        # 创建实例
        self.g = Graph(
            host="127.0.0.1",  # neo4j 搭载服务器的ip地址，ifconfig可获取到
            http_port=7474,  # neo4j 服务器监听的端口号
            user="neo4j",  # 数据库user name，如果没有更改过，应该是neo4j
            password="neo4jzl")

    def write(self, csvfile, message):  # 写入csv数据
        newfile = open(csvfile, 'a+', newline='')
        filewriter = csv.writer(newfile)
        filewriter.writerows(message)

    def open(self, file):
        self.file = file
        with open(self.file, 'r') as f:
            self.reader = csv.reader(f)
            self.messages = list(self.reader)
            print(self.messages)

    def tra_attribute(self):  # 经测试，neo4j的类型名、属性内容可以是中文，但属性名必须是英文。因此先要把所有的属性名全部翻译成英文。
        list_attribute = []
        for messages in self.messages[1:]:
            for message in messages:
                if r'#' in message:
                    for i in message.split(r'@@'):
                        if r'#' in i:
                            node_attribute = i.split(r'#')
                            list_attribute.append(node_attribute[0])
        list_attribute = list(set(list_attribute))
        lists_attribute = []
        for i in list_attribute:
            j = []
            j.append(i)
            j.append('')
            lists_attribute.append(j)
        self.write(csvfile='C:\\chouquxinxi\\出行指南\\属性名.csv', message=lists_attribute)
        # 后面需要手动打开该文件，手动翻译属性名，写在第二列。弄了半天自动翻译没弄成，有点尴尬。

    def cypher_make_send(self):  # 核心方法
        # 读取翻译结果
        dict_attribute = {}
        with open('C:\\chouquxinxi\\出行指南\\属性名.csv', 'r') as f:
            reader = csv.reader(f)
            for l in reader:
                dict_attribute[l[0]] = l[1]

        # 判断每行数据信息
        for message in self.messages[1:]:
            if len(message) == 2 or message[2] == '':  # 2列信息描述结点
                node_type = message[0]       # 节点类型
                node_attributes = message[1]    # 节点属性
                dict_node_attribute = {}  # 属性字典，下面做这个字典
                for i in node_attributes.split(r'@@'):
                    node_attribute = i.split(r'#')     # 节点属性分段，node_attribute[0]属性名，node_attribute[1]属性值
                    node_attribute[0] = dict_attribute[node_attribute[0]]  # 属性名翻译为英文
                    dict_node_attribute[node_attribute[0]] = node_attribute[1]  # 转为字典
                # print(dict_node_attribute)
                if '名称' in node_attributes:  # 如果这一条信息里面有“名称”字样，则说明该信息是对单个节点进行操作。
                    # 准备这么操作：建立该节点，然后一条属性一条属性的添加
                    namevalue = dict_node_attribute.pop('name')  # 删除要删除的键值对"name",返回值是删除的值name的内容。剩下的就是需要创建或修订的其他属性
                    cypher = "MERGE (m:%s { name: '%s' })  RETURN m.name" % (node_type, namevalue)
                    # 上一句的意思是，如果该节点类型、名称不存在，则创建该节点。
                    re = self.g.run(cypher).data()
                    print("-->节点<--新建节点：%s" % (re))
                    for key, value in dict_node_attribute.items():
                        cypher = "MERGE (m:%s { name: '%s' })   \
                                ON MATCH SET m.%s = '%s'    \
                                RETURN m.%s" % (node_type, namevalue, key, value, key)
                        re = self.g.run(cypher).data()
                        print("-->节点<--'%s':新建或修改属性：%s" % (namevalue, re))
                else:  # 如果不包含“名称”字样，则说明该信息是所有该类型的节点进行操作
                    for key, value in dict_node_attribute.items():
                        cypher = "MERGE (m:%s)              \
                                ON MATCH SET m.%s = '%s'     \
                                RETURN m.%s" % (node_type, key, value, key)
                        re = self.g.run(cypher).data()
                        print("-->节点<--新建或修改属性：%s" % (re))

            elif len(message) == 3:  # 3列信息描述关系
                relationships = message[0]  # 关系
                dict_relationships = {}  # 关系字典，下面做这个字典
                if r'@@' in relationships:  # 如果包含分割符，说明包含属性，此时需要把属性读取出来
                    list_relationships = relationships.split(r'@@')
                    dict_relationships['type'] = list_relationships[0]  # 关系类型
                    for i in list_relationships[1:]:
                        relationships_attribute = i.split(r'#')
                        relationships_attribute[0] = dict_attribute[relationships_attribute[0]]  # 属性名翻译为英文
                        dict_relationships[relationships_attribute[0]] = relationships_attribute[1]  # 转为字典
                else:
                    dict_relationships['type'] = relationships

                node_start = message[1]  # 起节点
                dict_node_start = {}  # 起节点属性字典，下面做这个字典
                for i in node_start.split(r'@@'):
                    node1_attribute = i.split(r'#')
                    node1_attribute[0] = dict_attribute[node1_attribute[0]]  # 属性名翻译为英文
                    dict_node_start[node1_attribute[0]] = node1_attribute[1]  # 转为字典

                node_end = message[2]  # 止节点
                dict_node_end = {}  # 止节点属性字典，下面做这个字典
                for i in node_end.split(r'@@'):
                    node2_attribute = i.split(r'#')     # 节点属性分段，node_attribute[0]属性名，node_attribute[1]属性值
                    node2_attribute[0] = dict_attribute[node2_attribute[0]]  # 属性名翻译为英文
                    dict_node_end[node2_attribute[0]] = node2_attribute[1]  # 转为字典

                #下面开始写语句

                start_type = ''
                start_attribute = ''
                if 'type' in dict_node_start:  # 如果包含类型
                    start_type = ":%s" % (dict_node_start['type'])
                    dict_node_start.pop('type')
                for key, value in dict_node_start.items():
                    start_attribute = start_attribute + "%s:'%s'" % (key, value) + ','
                if start_attribute == '':
                    cypher_node_start = start_type
                else:
                    cypher_node_start = start_type+'{'+start_attribute[:-1]+'}'  #[:-1]把最后一个逗号去掉

                end_type = ''
                end_attribute = ''
                if 'type' in dict_node_end:  # 如果包含类型
                    end_type = ":%s" % (dict_node_end['type'])
                    dict_node_end.pop('type')
                for key, value in dict_node_end.items():
                    end_attribute = end_attribute + "%s:'%s'" % (key, value) + ','
                if end_attribute == '':
                    cypher_node_end = end_type
                else:
                    cypher_node_end = end_type + '{' + end_attribute[:-1] + '}'

                relationships_type = ''
                relationships_attribute = ''
                if 'type' in dict_relationships:  # 如果包含类型
                    relationships_type = ":%s" % (dict_relationships['type'])
                    dict_relationships.pop('type')
                for key, value in dict_relationships.items():
                    relationships_attribute = relationships_attribute + "%s:'%s'" % (key, value) + ','
                if relationships_attribute == '':
                    cypher_relationships = relationships_type
                else:
                    cypher_relationships = relationships_type + '{' + relationships_attribute[:-1] + '}'

                cypher = " MATCH(m" + cypher_node_start + "),(n" + cypher_node_end + ")  MERGE(m)-[r" + cypher_relationships +"]->(n)   RETURN m,n,r"
                re = self.g.run(cypher).data()
                print("-->关系<--新建或修改关系：%s" % (re))

if __name__ == '__main__':
    file = 'C:\\chouquxinxi\\出行指南\\车票.csv'
    read = MAIN()
    read.open(file=file)
    #read.tra_attribute()  #第一步,运行完毕后手动翻译
    read.cypher_make_send()  #第二步



