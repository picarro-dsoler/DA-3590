import numpy as np


def most_frequent(occurrences):
    # find and return the most frequent occurrence in the input list
    return max(set(occurrences), key=occurrences.count)


def cdfpdf(pdfData, nBins, weights=None):
    """
    Comptue the cumulative and probability distribution functions of an input array.
    pdfData - input dataset
    nBins - number of bins on a linear scale with constant dx, or a
            sequence of scalars defining the bin edges without constant dx
    """
    pdfData = np.sort(pdfData[~np.isnan(pdfData)])
    assert len(pdfData) > 1
    if weights is None:
        weights = np.ones(len(pdfData))
    PDF, binEdges = np.histogram(pdfData, bins=nBins, weights=weights, density=True)

    if type(binEdges) in (int, float):
        CDF = np.cumsum(PDF/sum(PDF))
    else:
        nData = len(pdfData)
        CDF = np.asarray([np.sum(pdfData < x) / nData for x in binEdges[1::]])

    binCenters = binEdges[:-1] + np.diff(binEdges)/2.0

    return CDF, PDF, binCenters
