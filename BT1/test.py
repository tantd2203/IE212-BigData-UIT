import sys
input = sys.stdin.read

def main():
    # Reading input
    data = input().split()
    reload_1 = int(data[0])
    reload_all = int(data[1])
    capability = int(data[2])
    enemy = int(data[3])
    fire_time = int(data[4])

    ans = 0

    if reload_1 < reload_all / capability:
        ans = (enemy - capability) * reload_1 + enemy * fire_time
    else:
        if enemy % capability == 0:
            ans = enemy * fire_time + (enemy // capability - 1) * reload_all
        else:
            ans = enemy * fire_time + (enemy // capability - 1) * reload_all + min(enemy % capability * reload_1, reload_all)

    print(ans)

if __name__ == "__main__":
    main()
