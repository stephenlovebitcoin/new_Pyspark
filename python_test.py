import re

class Solution:
    def get_student_score(self,student_id,score):
        result = student_id.title() + ',' + str(score)
        return result

    def get_items(self,people,item):
        try:
            for item in people:
                for key,value in item.items():
                    print(item['name']+' '+str(item['score'])+'\n')
        except:
            pass
    def regex_greedy(self,str):
        result = re.findall('Friday\d+?',str)

        return result
    def regex_notgreedy(self,str):
        result = re.findall('Friday\d+',str)

        return result

    def regex_group(self,str):
        result = re.findall("(name){2}a(98|100)",str)

        return result

    def regex_search_group(self,str):
        result = re.search("(name){2}a(98|100)",str)

        return result

if __name__=='__main__':
    s = Solution()
    print(s.get_student_score('111',90))
    student={
        'name':'Tom',
        'age':20,
        'score':90
    }
    teacher={
        'name':'Jim',
        'age':50,
        'score':None
    }
    people = [student,teacher]
    s.get_items(people,student)
    print(s.regex_greedy('Friday20210917'))
    print(s.regex_notgreedy('Friday20210917'))
    print(s.regex_group('namenamea10298100'))
    print(s.regex_group('namenamea10098100'))
    print(s.regex_search_group('namenamea10098100'))
    print(s.regex_search_group('namenamea10298100'))





# 贪婪模式:尽可能匹配最多的字符
# 非贪婪模式:尽可能少的匹配字符
#
# 贪婪模式改成非贪婪模式使用?,在+*?{}后面添加?可以变成非贪婪
