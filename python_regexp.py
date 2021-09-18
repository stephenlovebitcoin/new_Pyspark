import re

class Regex:
    def replace_str(self,str):
        return re.sub('[A-Z]','12345',str)

    def replace_str_v2(self,str,fileIn):
        line = re.sub(
            r"(?i)^.*interfaceOpDataFile.*$",
            "interfaceOpDataFile %s" % fileIn,
            str
        )
        return line

    def search_str(self,str):
        reg= re.compile('[0-9]{13}')

        return reg.search(str)

    def split_str(self,str):
        res = re.split('[0-9]',str)
        return res

    def finall_str(self,f):
        name = re.findall('[a-z]{3}',f.read())
        return name

    def get_date(self,str):
        date = re.search(r'[0-9]{8}', str)

        return date
    def greedy_match(self,str):
        greedy_res = re.match("(HELLO|GOOD).*c",str)
        return greedy_res

    def greedy_findAll(self,str):
        greedy_res = re.findall("(HELLO|GOOD).*c",str)
        return greedy_res


if __name__=='__main__':
    reg = Regex()
    f = open("C:\\test\\student.csv",'r')
    print(reg.finall_str(f))
    print(reg.split_str('text1991498914941144aa478798aategdsg878978fdsg'))
    print(reg.search_str('text1991498914941144aa478798aategdsg878978fdsg'))
    print(reg.search_str('text1991498914941144aa478798aategdsg878978fdsg'))
    print(reg.get_date('123435_abb_00_abcd_20201108.csv'))
    print(reg.greedy_match('HELLOaaacGOOD56700'))
    print(reg.greedy_findAll('HELLOaaacGOOD56700'))

# re(re.MULTILINE)##search in all the text
# re(re.IGNORECASE)
# re(re.I)##ignore upper case or lower case
