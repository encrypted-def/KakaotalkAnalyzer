import pyodbc 
import datetime
import codecs
from konlpy.tag import Kkma
from konlpy.utils import pprint
import unicodedata

def preformat(string, width, align='<', fill=' '):
    string = str(string)
    count = (width - sum(1 + (unicodedata.east_asian_width(c) in "WF")
                         for c in string))
    return {
        '>': lambda s: fill * count + s,
        '<': lambda s: s + fill * count,
        '^': lambda s: fill * (count / 2)
                       + s
                       + fill * (count / 2 + count % 2)
}[align](string)

class UnknownSenderError(Exception):
    def __init__(self):
        pass

class FormatError(Exception):
    def __init__(self):
        pass

def formattedtime(year,month,day,hour,min): # 2017-10-2 21:3:41
    return year+"-"+month+"-"+day+" "+hour+":"+min

def parse_chatlog(filename):
    print("파싱을 시작합니다.")
    acceptedQuery = 0 # 처리된 쿼리의 수
    UnknownSenderQuery = 0
    DroppedQuery = 0
    f = codecs.open(filename, "r", "utf-8")
    beforeSender = ''
    beforeTime = datetime.datetime(1,1,1,1,1,1);
    cnt = 0
    for line in f:
        try:
            cnt += 1
            if cnt % 10000 == 0:
                print("{}번째 줄까지 파싱 완료했습니다.".format(cnt))
            query = line.rstrip() # 오른쪽 \r\n을 제거
            chunk = query.split(' ', 7)
            year = chunk[0][:-1]
            month = chunk[1][:-1]
            day = chunk[2][:-1]
            ampm = chunk[3]
            hour = chunk[4].split(':')[0]
            min = chunk[4].split(':')[1][:-1]
            sender = chunk[5]
            if chunk[6] != ':':
                raise FormatError
            msg = chunk[7]
            if sender == '(알수없음)':
                beforeSender = ''
                raise UnknownSenderError
            if not (year.isdigit() and month.isdigit() and hour.isdigit() and min.isdigit()): # 숫자가 아닌게 끼여있으면
                raise FormatError
            #오전 12시 37분이 곧 0시 37분, 오후 12시 37분이 곧 낮 12시 37분을 의미
            if ampm == '오전':
                if hour == '12': #오전 12시인 경우에만 0시로 변경함
                    hour == '00'
            elif ampm == '오후':
                if hour != '12': #오후 12시가 아닌 경우에만 12를 더해줌
                    hour = str(int(hour)+12)
            else: # '오전', '오후' 둘 다 아닐 경우
                raise FormatError
            # MESSAGE table insert
            currentTime = datetime.datetime(int(year), int(month), int(day), int(hour), int(min))
            TimeToStr = formattedtime(year,month,day,hour,min)          
            cursor.execute("insert into MESSAGE(id, msgtext, sendtime) values ({}, '{}', '{}');".format(acceptedQuery+1, msg, TimeToStr))            
            # SENDS table insert
            cursor.execute("insert into SENDS(sender, id) values ('{}', {});".format(sender, acceptedQuery+1))
            # USER table insert
            cursor.execute("select * from USER where name='{}' limit 1;".format(sender))
            rows = cursor.fetchall()        
            if not rows: # 이전에 나온 적 없는 사람이라면
                cursor.execute("insert into USER(name, jointime) values ('{}', '{}');".format(sender, TimeToStr)) # USER에 추가                        
            # CONVERSATION table insert
            if beforeSender and beforeSender != sender and (currentTime-beforeTime).total_seconds() < 600: # 600초(10분) 이내에 응답이 왔을 경우 대화로 취급
                cursor.execute("insert into CONVERSATION(sender, receiver, starttime) values ('{}', '{}', '{}');".format(beforeSender, sender, TimeToStr));
         
            beforeSender = sender;
            beforeTime = currentTime;
            acceptedQuery += 1   
        except UnknownSenderError: 
            UnknownSenderQuery += 1
        except:
            DroppedQuery += 1
    print("파싱을 완료했습니다.");
    print("처리한 쿼리 : {}".format(acceptedQuery))
    print("작성자가 탈퇴해서 처리하지 못한 쿼리 : {}".format(UnknownSenderQuery))
    print("그 외 오류로 처리하지 못한 쿼리 : {}".format(DroppedQuery))
    cnxn.commit() # DB 변경사항을 적용함
    f.close()

def UserRankingByDate(start, end):
    try:
        cursor.execute("SELECT count(*) from message WHERE '{}' <= date_format(sendtime, '%Y-%m-%d') AND date_format(sendtime, '%Y-%m-%d') <= '{}';".format(start,end))
    except:
        print("\nSQL에서 오류가 발생했습니다. 형식에 맞는 날짜를 입력했는지 확인해주세요.")
        return None
    QueryNum = int(cursor.fetchall()[0][0])
    if QueryNum == 0:
        print("\n({}) ~ ({}) 기간에 작성된 메시지가 없습니다.".format(start, end, QueryNum))
        return None
    try:
        cursor.execute("SELECT sender, count(sender) FROM message NATURAL JOIN sends WHERE '{}' <= date_format(sendtime, '%Y-%m-%d') AND date_format(sendtime, '%Y-%m-%d') <= '{}' GROUP BY sender ORDER BY count(sender) desc;".format(start, end))
        rows = cursor.fetchall()
        print("\n({}) ~ ({}) 기간의 메시지 : {}개".format(start, end, QueryNum))
        print("{}{}{}{}".format(preformat("순위", tab_sz),preformat("이름", tab_sz),preformat("갯수", tab_sz),preformat("비율", tab_sz)))
        rank = 1
        for row in rows:
            print("{}{}{}{:.2f}%".format(preformat(rank, tab_sz),preformat(row[0], tab_sz),preformat(row[1], tab_sz),float(preformat(100*row[1]/QueryNum, tab_sz))))
            rank += 1
    except:
        print("\nSQL에서 오류가 발생했습니다. 형식에 맞는 날짜를 입력했는지 확인해주세요.")

def UserRankingByHour(start, end):
    try:
        cursor.execute("SELECT count(*) from message WHERE {} <= extract(hour FROM sendtime) AND extract(hour FROM sendtime) <= {};".format(start,end))
    except:
        print("\nSQL에서 오류가 발생했습니다. 형식에 맞는 시간을 입력했는지 확인해주세요.")
        return None
    QueryNum = int(cursor.fetchall()[0][0])
    if QueryNum == 0:
        print("\n{}시 ~ {}시에 작성된 메시지가 없습니다.".format(start, end, QueryNum))
        return None
    try:
        cursor.execute("SELECT sender, count(sender) FROM message NATURAL JOIN sends WHERE {} <= extract(hour FROM sendtime) AND extract(hour FROM sendtime) <= {} GROUP BY sender ORDER BY count(sender) desc;".format(start, end))
        rows = cursor.fetchall()
        print("\n{}시 ~ {}시에 작성된 메시지 : {}개".format(start, end, QueryNum))
        print("{}{}{}{}".format(preformat("순위", tab_sz),preformat("이름", tab_sz),preformat("갯수", tab_sz),preformat("비율", tab_sz)))
        rank = 1
        for row in rows:
            print("{}{}{}{:.2f}%".format(preformat(rank, tab_sz),preformat(row[0], tab_sz),preformat(row[1], tab_sz),float(preformat(100*row[1]/QueryNum, tab_sz))))
            rank += 1
    except:
        print("SQL에서 오류가 발생했습니다. 형식에 맞는 날짜를 입력했는지 확인해주세요.")

def KeywordRankingByDate(start, end, limit):
    print("")
    cursor.execute("SELECT sender, msgtext, sendtime, id FROM message NATURAL JOIN sends WHERE '{}' <= date_format(sendtime, '%Y-%m-%d') AND date_format(sendtime, '%Y-%m-%d') <= '{}';".format(start, end))
    rows = cursor.fetchall()
    QueryNum = 0
    beforeDate = ''
    for row in rows:
        TrimMessage = ""
        for c in row[1]:
            if 0x3131 <= ord(c) <= 0x318F: # 단일 자음 모음(ex : ㄱㄱ) 제거. konlpy의 오류때문에 넣음
                continue
            TrimMessage += c
        TrimMessage = TrimMessage.strip()
        if not TrimMessage:
            continue
        if str(row[2])[:10] != beforeDate:
            print("{}일 대화 분석을 시작합니다.".format(str(row[2])[:10]))
            beforeDate = str(row[2])[:10]
        wordlist = set(kkma.nouns(TrimMessage)) # 같은 문장 내의 단어의 중복 제거를 위해
        for word in wordlist:
            try:
                if len(word) < 2:
                    continue
                if word == '사진' or word == '모티콘':
                    continue
                cursor.execute("INSERT INTO keyword(word, id) VALUES ('{}', {});".format(word, row[3]))
                QueryNum += 1
            except:
                print("키워드 추출 중 알 수 없는 오류가 발생했습니다. 개발자에게 문의해주세요.")
                return None
    cnxn.commit()
    try:
        cursor.execute("SELECT count(*) FROM keyword;")
    except:
        print("SQL에서 오류가 발생했습니다. 형식에 맞는 시간을 입력했는지 확인해주세요.")
        return None
    if QueryNum == 0:
        print("({}) ~ ({}) 기간에 추출된 키워드가 없습니다".format(start, end))
        return None
    try:
        cursor.execute("SELECT word, count(word) FROM keyword GROUP BY word ORDER BY count(word) desc limit {};".format(limit))
        rows = cursor.fetchall()
        print("\n({}) ~ ({}) 기간에 추출된 키워드 : {}개".format(start, end, QueryNum))
        print("{}{}{}{}".format(preformat("순위", tab_sz),preformat("키워드", tab_sz),preformat("갯수", tab_sz),preformat("비율", tab_sz)))
        rank = 1
        for row in rows:
            print("{}{}{}{:.2f}%".format(preformat(rank, tab_sz),preformat(row[0], tab_sz),preformat(row[1],tab_sz),float(preformat(100*row[1]/QueryNum, tab_sz))))
            rank += 1       
    except:
        print("SQL에서 오류가 발생했습니다. 형식에 맞는 시간을 입력했는지 확인해주세요.")
    try:
        cursor.execute("TRUNCATE TABLE keyword;")
        pass
    except:
        pass

def DateRanking(limit):
    try:
        cursor.execute("SELECT date_format(sendtime, '%Y-%m-%d'), count(date_format(sendtime, '%Y-%m-%d')) FROM message GROUP BY date_format(sendtime, '%Y-%m-%d') ORDER BY count(date_format(sendtime, '%Y-%m-%d')) desc limit {};".format(limit))
        rows = cursor.fetchall()
        print("{}{}{}".format(preformat("순위", tab_sz),preformat("날짜", tab_sz),preformat("갯수", tab_sz)))
        rank = 1
        for row in rows:
            print("{}{}{}".format(preformat(rank, tab_sz),preformat(row[0], tab_sz),preformat(row[1], tab_sz)))
            rank += 1
    except:
        print("SQL에서 오류가 발생했습니다. 형식에 맞는 시간을 입력했는지 확인해주세요.")

def ReceiverRankingByUser(sender):
    print("")
    QueryNum = 0
    try:
        cursor.execute("SELECT count(*) FROM conversation WHERE sender='{}';".format(sender))
        QueryNum = int(cursor.fetchall()[0][0])
        if QueryNum == 0:
            raise NotExistError
        cursor.execute("SELECT receiver, count(receiver) FROM conversation WHERE sender = '{}' GROUP BY receiver ORDER BY count(receiver) desc;".format(sender));
        rows = cursor.fetchall()
        print("'{}'님이 시작한 대화를 받아준 사람".format(sender))
        print("{}{}{}{}".format(preformat("순위", tab_sz),preformat("이름", tab_sz),preformat("갯수", tab_sz),preformat("비율", tab_sz)))
        rank = 1
        for row in rows:
            print("{}{}{}{:.2f}%".format(preformat(rank, tab_sz),preformat(row[0], tab_sz),preformat(row[1], tab_sz),float(preformat(100*row[1]/QueryNum, tab_sz))))
            rank += 1      
    except:
        print("채팅방에 없는 사람이거나 대화를 한 번도 하지 않은 사람입니다.")

def SenderRankingByUser(receiver):
    print("")
    QueryNum = 0
    try:
        cursor.execute("SELECT count(*) FROM conversation WHERE receiver='{}';".format(receiver))
        QueryNum = int(cursor.fetchall()[0][0])
        if QueryNum == 0:
            raise NotExistError
        cursor.execute("SELECT sender, count(sender) FROM conversation WHERE receiver = '{}' GROUP BY sender ORDER BY count(sender) desc;".format(receiver));
        rows = cursor.fetchall()
        print("'{}'님이 대화를 받아준 사람".format(receiver))
        print("{}{}{}{}".format(preformat("순위", tab_sz),preformat("이름", tab_sz),preformat("갯수", tab_sz),preformat("비율", tab_sz)))
        rank = 1
        for row in rows:
            print("{}{}{}{:.2f}%".format(preformat(rank, tab_sz),preformat(row[0], tab_sz),preformat(row[1], tab_sz),float(preformat(100*row[1]/QueryNum, tab_sz))))
            rank += 1      
    except:
        print("채팅방에 없는 사람이거나 대화를 한 번도 하지 않은 사람입니다.")

def printintro():
    print("\n---------------------------------------------------------")
    print("1. 새로운 대화 불러오기")
    print("2. 특정 기간 내의 대화 참여자 랭킹 보기")
    print("3. 시간대별 대화 참여자 랭킹 보기")
    print("4. 특정 기간 내의 키워드 랭킹 보기")
    print("5. 가장 톡방이 활발했던 날짜 보기")
    print("6. 특정 사람의 대화를 받아준 사람의 랭킹 보기")
    print("7. 특정 사람이 누구의 대화를 잘 받아줬는지에 대한 랭킹 보기")
    print("8. 불러온 대화 제거하기")
    print("---------------------------------------------------------")

tab_sz = 15
print("카카오톡 대화 분석기")
print("로딩중...\n\n")
server = 'localhost' 
database = 'test10' 
username = 'root' 
password = 'root'
cnxn = pyodbc.connect('DRIVER={MySQL ODBC 5.3 ANSI Driver};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
kkma = Kkma()
kkma.nouns("테스트") # 단어 로딩을 위해
LoadedFile = ""
while True:
    printintro()
    try:
        choice = int(input("> "))
        if choice == 1:
            if LoadedFile:
                print("로그파일 {}이 이미 로드되어 있습니다. 제거 후에 다시 로드해주세요.".format(LoadedFile))
                continue
            print("로그파일 이름을 입력해주세요.")
            filename = input("> ")
            try:
                open(filename, "r")                
            except:
                print("파일을 열 수 없습니다.")
                continue
            else:
                LoadedFile = filename
                parse_chatlog(LoadedFile)
        elif choice == 2:
            if not LoadedFile:
                print("로딩된 로그파일이 없습니다.")
                continue
            start = '0001-01-01'
            end = '9999-01-01'
            print("전체 기간에 대해 확인하겠습니까? (Y/N)")
            choiceYN = input("> ")
            if choiceYN == 'Y':
                pass
            elif choiceYN == 'N':
                print("기간의 시작 날짜와 끝 날짜를 아래 형식대로 입력해주세요")
                print("ex) 2016-03-03~2017-04-07")
                duration = input("> ")
                try:
                    start, end = duration.split("~")
                except:
                    print("오류가 발생했습니다. 기간을 형식에 맞게 입력했는지 확인해주세요.")
                    continue
            else:
                print("잘못 입력하셨습니다.")
                continue
            UserRankingByDate(start, end)
        elif choice == 3:
            if not LoadedFile:
                print("로딩된 로그파일이 없습니다.")
                continue
            start = '00'
            end = '24'
            print("전체 시간대에 대해 확인하겠습니까? (Y/N)")
            choiceYN = input("> ")
            if choiceYN == 'Y':
                pass
            elif choiceYN == 'N':
                print("시간대의 시작 시간과 끝 시간을 아래 형식대로 입력해주세요")
                print("ex1) 00~06 ex2) 18~24")
                duration = input("> ")
                try:
                    start, end = duration.split("~")
                except:
                    print("오류가 발생했습니다. 시간을 형식에 맞게 입력했는지 확인해주세요.")
                    continue
            else:
                print("잘못 입력하셨습니다.")
                continue
            UserRankingByHour(start, end)
        elif choice == 4:
            if not LoadedFile:
                print("로딩된 로그파일이 없습니다.")
                continue
            start = '0001-01-01'
            end = '9999-01-01'            
            print("기간이 길 경우 분석 시간이 오래 걸릴 수 있습니다. 한달 이내의 기간으로 확인하는 것을 권장합니다.")
            print("기간의 시작 날짜와 끝 날짜를 아래 형식대로 입력해주세요")
            print("ex) 2017-04-01~2017-04-07")
            duration = input("> ")
            try:
                (start, end) = tuple(duration.split("~"))
            except:
                print("오류가 발생했습니다. 기간을 형식에 맞게 입력했는지 확인해주세요.")
                continue
            print("최대 몇 위 까지의 키워드를 보고싶은지 입력해주세요.")
            try:
                limit = int(input("> "))
            except:
                print("오류가 발생했습니다. 수가 아닌 다른 값을 입력한 것으로 추정됩니다.")
                continue
            KeywordRankingByDate(start, end, limit)
        elif choice == 5:
            if not LoadedFile:
                print("로딩된 로그파일이 없습니다.")
                continue
            print("최대 몇 위 까지의 날짜를 보고싶은지 입력해주세요.")
            try:
                limit = int(input("> "))
            except:
                print("오류가 발생했습니다. 수가 아닌 다른 값을 입력한 것으로 추정됩니다.")
                continue
            DateRanking(limit)
        elif choice == 6:
            if not LoadedFile:
                print("로딩된 로그파일이 없습니다.")
                continue
            print("확인하고 싶은 사람의 이름을 입력하세요.")
            sender = input("> ")
            ReceiverRankingByUser(sender)
        elif choice == 7:
            if not LoadedFile:
                print("로딩된 로그파일이 없습니다.")
                continue
            print("확인하고 싶은 사람의 이름을 입력하세요.")
            receiver = input("> ")
            SenderRankingByUser(receiver)
        elif choice == 8:
            try:
                if not LoadedFile:
                    print("로딩된 로그파일이 없습니다. 그래도 데이터베이스를 초기화하시겠습니까? Y/N")
                    choiceYN = input("> ")
                    if choiceYN == 'Y':
                        pass
                    elif choiceYN == 'N':
                        continue
                    else:
                        print("잘못 입력하셨습니다.")
                        continue
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
                cursor.execute("TRUNCATE TABLE conversation;")
                cursor.execute("TRUNCATE TABLE sends;")
                cursor.execute("TRUNCATE TABLE keyword;")
                cursor.execute("TRUNCATE TABLE message;")
                cursor.execute("TRUNCATE TABLE user;")

                LoadedFile = ""
                print("대화의 제거가 완료되었습니다.\n");
            except:
                print("제거에 실패했습니다.")
        else:
            raise WrongNumberError

    except:
        print("잘못 입력했습니다.")
            
