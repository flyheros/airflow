def get_sftp():
    print('sftp 작업을 시작합니다 ')


def regist(name, sex, *args):
    print(f'이름 : {name}')
    print(f'성별 : {sex}')
    print(f'기타타 : {args}')

def regist2(name, sex, *args,  **kwargs):
    print(f'이름 : {name}')
    print(f'성별 : {sex}')
    print(f'기타타 : {args}')
    email = kwargs['email'] or None
    address = kwargs['address'] or None
    print(f'이메일은 {email}')
    print(f'주소는 {address}')