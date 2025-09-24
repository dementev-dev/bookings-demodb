from math import pi, cos, sin, acos, asin
import sys

def locside(side1, side2, angle):
    return acos(cos(side1)*cos(side2) + sin(side1)*sin(side2)*cos(angle))

def locangle(side1, side2, side3):
    t = (cos(side1) - cos(side2) * cos(side3)) / (sin(side2) * sin(side3))
    if (t < -1.0):
        t = -1.0   
    if (t > 1.0):
        t = 1.0   
    return acos(t)

steps = 20

l = 0
for line in sys.stdin:
    if l % 3 == 0:
        (lon1,lat1) = [float(x) for x in line.split()]
    elif l % 3 == 1:
        (lon2,lat2) = [float(x) for x in line.split()]
        rd = pi/180.0
        a = 0.5 * pi - lat1 * rd
        beta1 = lon1 * rd
        c = 0.5 * pi - lat2 * rd
        beta2 = lon2 * rd
        b = locside(a,c,(beta1 - beta2))
        alpha = locangle(a,c,b)
        for i in range(steps+1):
            lat = (0.5 * pi - locside(b * i / steps, c, alpha)) / rd
            lon = (beta2 + (-1 if sin(beta1 - beta2) < 0 else 1) *
                locangle(b * i / steps, locside(b * i / steps, c, alpha), c)) / rd
            print( str(lon) + " " + str(lat) )
    print( "\n" )
    l += 1

