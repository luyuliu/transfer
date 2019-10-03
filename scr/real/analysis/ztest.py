
from numpy import sqrt, abs, round
from scipy.stats import norm
def twoSampZ(X1, X2, mudiff, sd1, sd2, n1, n2):
    pooledSE = sqrt(sd1**2/n1 + sd2**2/n2)
    z = ((X1 - X2) - mudiff)/pooledSE
    pval = 2*(norm.sf(abs(z)))
    return round(z, 3), round(pval, 4)

if __name__ == "__main__":
    z, p = twoSampZ(28, 33, 0, 14.1, 9.5, 61141527, 62234281)
    print (z, p)