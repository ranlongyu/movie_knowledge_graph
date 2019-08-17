def do():
    for item in range(10):
        yield item

d=do()
for bi in range(3):
    print(bi)
    print(next(d))
a = []
if a:
    print('yes')