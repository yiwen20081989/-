import json
from py2neo import Graph, Node, Relationship, NodeMatcher


class MedicalGraph(object):  # 需要写父类吗？
    def __init__(self, file_path, url, username, pwd):
        self.data_path = file_path
        link = Graph(url, username=username, password=pwd)
        self.graph = link
        print("Graph linked!")
        self.graph.delete_all()
        self.matcher = NodeMatcher(link)

    #     1、解析json数据
    def read_nodes(self):
        """
        分别读取六大类型的节点数据，存放进对应列表
        以列表形式存入疾病的字典：明确疾病的属性
        节点关系存储
        返回七大类节点的集合、关系、疾病信息的列表
        """
        # 七大类型节点
        diseases = []
        symptoms = []
        departments = []
        check = []
        foods = []
        drugs = []

        disease_infos = []  # 疾病信息,元素为字典

        # 构建节点实体关系
        rels_department = []  # 科室－科室关系
        rels_noteat = []  # 疾病－忌吃食物关系
        rels_doeat = []  # 疾病－宜吃食物关系
        rels_recommandeat = []  # 疾病－推荐吃食物关系
        rels_commonddrug = []  # 疾病－通用药品关系
        rels_recommanddrug = []  # 疾病－热门药品关系
        rels_check = []  # 疾病－检查关系

        rels_symptom = []  # 疾病症状关系
        rels_acompany = []  # 疾病并发关系
        rels_category = []  # 疾病与科室之间的关系
        count = 0
        for data in open(self.data_path):
            #             print(data)
            data_json = json.loads(data)  # load是什么方法 报错
            disease_dict = {}
            count += 1
            print(count)
            disease = data_json['name']
            diseases.append(disease)
            # 疾病信息
            disease_dict['name'] = disease
            disease_dict['desc'] = ''
            disease_dict['prevent'] = ''
            disease_dict['cause'] = ''
            disease_dict['easy_get'] = ''
            disease_dict['cure_department'] = ''

            if 'symptom' in data_json:
                symptoms += data_json['symptom']
                for symptom in data_json['symptom']:
                    rels_symptom.append([disease, symptom])

            if 'acompany' in data_json:
                for acompany in data_json['acompany']:
                    rels_acompany.append([disease, acompany])

            if 'desc' in data_json:
                disease_dict['desc'] = data_json['desc']

            if 'prevent' in data_json:
                disease_dict['prevent'] = data_json['prevent']

            if 'cause' in data_json:
                disease_dict['cause'] = data_json['cause']

            if 'get_prob' in data_json:
                disease_dict['get_prob'] = data_json['get_prob']

            if 'easy_get' in data_json:
                disease_dict['easy_get'] = data_json['easy_get']

            if 'cure_department' in data_json:
                cure_department = data_json['cure_department']
                if len(cure_department) == 1:
                    rels_category.append([disease, cure_department[0]])
                if len(cure_department) == 2:
                    big = cure_department[0]
                    small = cure_department[1]
                    rels_department.append([small, big])
                    rels_category.append([disease, small])

                disease_dict['cure_department'] = cure_department
                departments += cure_department
            if 'common_drug' in data_json:
                common_drug = data_json['common_drug']
                for drug in common_drug:
                    rels_commonddrug.append([disease, drug])
                drugs += common_drug

            if 'recommand_drug' in data_json:
                recommand_drug = data_json['recommand_drug']
                drugs += recommand_drug
                for drug in recommand_drug:
                    rels_recommanddrug.append([disease, drug])

            if 'not_eat' in data_json:
                not_eat = data_json['not_eat']
                for _not in not_eat:
                    rels_noteat.append([disease, _not])

                foods += not_eat
                do_eat = data_json['do_eat']
                for _do in do_eat:
                    rels_doeat.append([disease, _do])

                foods += do_eat
                recommand_eat = data_json['recommand_eat']

                for _recommand in recommand_eat:
                    rels_recommandeat.append([disease, _recommand])
                foods += recommand_eat

            if 'check' in data_json:
                check_tep = data_json['check']
                for _check in check_tep:
                    rels_check.append([disease, _check])
                check += check_tep

            disease_infos.append(disease_dict)
            # 节点存储
        return set(diseases), set(symptoms), set(departments), set(check), set(foods), set(drugs), disease_infos, \
               rels_check, rels_recommandeat, rels_noteat, rels_doeat, rels_department, rels_commonddrug, rels_recommanddrug, \
               rels_symptom, rels_acompany, rels_category

    #     2、节点创建
    def node_create(self):
        diseases, symptoms, departments, check, foods, drugs, disease_infos, rels_check, rels_recommandeat, rels_noteat, rels_doeat, rels_department, rels_commonddrug, rels_recommanddrug, \
        rels_symptom, rels_acompany, rels_category = self.read_nodes()
        self.create_diseases_nodes(disease_infos)  # 疾病信息节点
        self.create_node('Symptoms', symptoms)  # 症状节点
        self.create_node('Departments', departments)  # departments节点
        self.create_node('Check', check)  # check节点
        self.create_node('Foods', foods)  # foods节点
        self.create_node('Drugs', drugs)  # drugs节点

    #     创建知识图谱中心疾病的节点
    def create_diseases_nodes(self, disease_infos):
        count = 0
        for disease_dict in disease_infos:
            node = Node("Disease", name=disease_dict['name'], desc=disease_dict['desc'],
                        prevent=disease_dict['prevent'], cause=disease_dict['cause'],
                        easy_get=disease_dict['easy_get'], cure_department=disease_dict['cure_department'])
            self.graph.create(node)
            count += 1
            print(count)
        return

    #     建立节点
    def create_node(self, label, nodes):
        count = 0
        for node_name in nodes:
            node = Node(label, name=node_name)
            self.graph.create(node)
            count += 1
            print(count, len(nodes))
        return

    # 4、关系创建
    def rels_create_graph(self):
        diseases, symptoms, departments, check, foods, drugs, disease_infos, rels_check, rels_recommandeat, rels_noteat, rels_doeat, rels_department, rels_commonddrug, rels_recommanddrug, \
        rels_symptom, rels_acompany, rels_category = self.read_nodes()
        self.rels_create_node("Disease", "Check", rels_check, "rels_check", "检查")
        self.rels_create_node("Disease", "Foods", rels_recommandeat, "rels_recommandeat", "推荐食谱")
        self.rels_create_node("Disease", "Foods", rels_noteat, "rels_noteat", "宜吃")
        self.rels_create_node("Disease", "Foods", rels_doeat, "rels_doeat", "忌吃")
        self.rels_create_node("Disease", "Drugs", rels_commonddrug, "rels_commonddrug", "一般治疗药物")
        self.rels_create_node("Disease", "Drugs", rels_recommanddrug, "rels_recommanddrug", "推荐用药")
        self.rels_create_node("Disease", "Symptoms", rels_symptom, "rels_symptom", "症状")
        self.rels_create_node("Disease", "Disease", rels_acompany, "rels_acompany", "并发症")
        self.rels_create_node("Disease", "'Department', ", rels_category, "rels_category", "所属科室")
        self.rels_create_node("Departments", "Departments", rels_department, "belongs_to", "属于")

    def rels_create_node(self, label1, label2, rels_list, rels_type,rels_name):
        for rel in rels_list:
            i = rel[0]
            j = rel[1]
            if j.strip() == '穿心莲内酯片':
                print("打印列表")
                print(i, j)
            try:  # matcher 支持where子句查询
                r = Relationship(self.matcher.match(label1, name=i).first(),
                                 rels_type, self.matcher.match(label2, name=j).first(),name=rels_name)
                self.graph.create(r)
            except AttributeError as e:
                print("注意，有问题！")
                print(e, i, j)

    #  5、读取节点，导出到txt
    def export_data(self):
        diseases, symptoms, departments, check, foods, drugs, disease_infos, rels_check, rels_recommandeat, rels_noteat, rels_doeat, rels_department, rels_commonddrug, rels_recommanddrug, \
        rels_symptom, rels_acompany, rels_category = self.read_nodes()
        f_drug = open('dict/drug.txt', 'w+', encoding='utf-8')
        f_food = open('dict/food.txt', 'w+', encoding='utf-8')
        f_check = open('dict/check.txt', 'w+', encoding='utf-8')
        f_department = open('dict/department.txt', 'w+', encoding='utf-8')
        f_symptom = open('dict/symptoms.txt', 'w+', encoding='utf-8')
        f_disease = open('dict/disease.txt', 'w+', encoding='utf-8')
        f_commonddrug = open('dict/rels_commonddrug.txt', 'w+', encoding='utf-8')

        f_drug.write('\n'.join(list(drugs)))
        f_food.write('\n'.join(list(foods)))
        f_check.write('\n'.join(list(check)))
        f_department.write('\n'.join(list(departments)))
        f_symptom.write('\n'.join(list(symptoms)))
        f_disease.write('\n'.join(list(diseases)))
        f_commonddrug.write('\n'.join('%s' % id for id in list(rels_commonddrug)))

        f_drug.close()
        f_food.close()
        f_check.close()
        f_department.close()
        f_symptom.close()
        f_disease.close()
        f_commonddrug.close()
        return


if __name__ == "__main__":
    data_path = "data\medical2.json"
    g_url = "bolt://localhost:7687"
    g_username = "neo4j"  # 修改neo4j数据库用户名
    g_pwd = "neo4j12345"  # 修改neo4j数据库密码
    handler = MedicalGraph(data_path, g_url, g_username, g_pwd)
    handler.node_create()
    handler.rels_create_graph()
    handler.export_data()
