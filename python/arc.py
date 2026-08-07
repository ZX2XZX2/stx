import argparse
import math

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--total", type=int)
    parser.add_argument("--p1", type=int)
    parser.add_argument("--p2", type=int)
    parser.add_argument("--p3", type=int)
    parser.add_argument("--p4", type=int)
    parser.add_argument("--p5", type=int)
    args = parser.parse_args()
    total = args.total
    p1 = round(1.9 * args.p1 + 0.1)
    p2 = round(1.9 * args.p2 + 0.1)
    p3 = round(1.9 * args.p3 + 0.1)
    p4 = round(1.9 * args.p4 + 0.1)
    p5 = round(1.9 * args.p5 + 0.1)
    print(f"P5({p5}) P4({p4}) P3({p3}) P2({p2}) P1({p1})")
    r1 = total - 2 * p1
    r2 = total - p1 - 2 * p2
    r3 = total - p1 - p2 - 2 * p3
    r4 = total - p1 - p2 - p3 - 2 * p4
    r5 = total - p1 - p2 - p3 - p4 - 2 * p5

    print(f"{r1} {r2} {r3} {r4} {r5}")
