import sys


def foo(name: str):
    print(f'Hello {name}')


if __name__ == '__main__':
    name = sys.argv[1]
    foo(name=name)
