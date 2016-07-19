#!/usr/bin/env python
# encoding: utf-8
#
# create_plPlugMapM_LCO.py
#
# Created by José Sánchez-Gallego on 6 May 2016.
# Licensed under a 3-clause BSD license.
#
# Revision history:
#    6 May 2016 J. Sánchez-Gallego
#       Initial version


from __future__ import division
from __future__ import print_function
import sys
import os
from sdss.utilities import yanny
import numpy as np
import argparse
import string


cart = 20
nGuides = 16

# Links field numbers to marking colours
colourDict = {1: 'GREEN', 2: 'BLUE', 3: 'VIOLET'}

template = """
EVILSCAN
fscanVersion $HeadURL: https://svn.sdss.org/repo/operations/general/idlmapper/v6_0_7/src/evilscan.c $
pluggers     Jose
plateId      {plateID}
fscanMJD     {mjd}
fscanId      {fscanID}
fscanDate    Fri May  6 12:00:00 2016
fscanFile    fiberScan-{plateID}-57514-{fscanID:02d}.par
fscanMode    interpolated
fscanSpeed   400
fscanRows    960
fscanCols    960
fscanBias    45.000000
motorId1     35
motorId2     31
motorId3     3
cartridgeId  {cart}
fieldNumber  {field}
fieldColour  {fieldColour}
fmapVersion NOCVS:v6_0_7
idlutilsVersion v5_5_17
idlVersion 7.1
"""


def addHeader(header, file, plateID, fscanID, field, mjd, fieldColour):
    """Adds a header to a yanny file after the commented section."""

    fscanData = template.format(plateID=plateID, mjd=mjd, fscanID=fscanID,
                                cart=cart, field=field,
                                fieldColour=fieldColour)

    header = [''] + header + fscanData.splitlines() + ['']

    fileLines = open(file, 'r').read().splitlines()

    for ii, line in enumerate(fileLines):
        if not line.startswith('#'):
            break

    for line in header[::-1]:
        fileLines.insert(ii, line)

    unit = open(file, 'w')
    for line in fileLines:
        unit.write(line + '\n')
    unit.close()

    return


def getLookupArray(lookupTable):
    """Returns an array of fibres to holes."""

    # First column are fibres, seecond are holes
    # (i.e., the fibres in the original order of the plPlugMapP file)

    if lookupTable is None:
        return np.array([np.arange(1, 17), np.arange(1, 17)]).T

    return np.loadtxt(lookupTable).astype(np.int)


def create_plPlugMapM_LCO(plateID, pointing, field, mjd,
                          lookupTable=None, fscanId=1):
    """Converts the plPlugMapP files for a `plateID` into a plPlugMapM.

    This scripts converts the plPlugMapP files for a `plateID` into a
    plPlugMapM given a lookup table that simulates a mapping.

    Each guider commissioning plate contains four pointings, each with 3
    sets of 16 guiding stars (fields).

    """

    lookupArray = getLookupArray(lookupTable)

    pointingName = '' if pointing == 'A' else pointing

    filename = os.path.join(
        os.environ['PLATELIST_DIR'], 'plates',
        '{0:06d}'.format(plateID)[:-2] + 'XX', '{0:06d}'.format(plateID),
        'plPlugMapP-{0:d}{1}.par'.format(plateID, pointingName))

    assert os.path.exists(filename)

    yannyFile = yanny.yanny(filename, np=True)
    rawFile = open(filename, 'r').read().splitlines()

    plPlugMapObj = yannyFile['PLUGMAPOBJ']

    # Manually retrieves the header from the raw lines.
    header = []
    for line in rawFile:
        if line.strip().startswith('typedef enum {'):
            break
        header.append(line)

    # Calculates the range of fiberIds that correspond to this pointing
    # and fscanID.
    pointingNum = string.uppercase.index(pointingName)
    preIndex = nGuides * 3 * pointingNum
    fiberID_range = preIndex + np.arange(1 + (field - 1) * nGuides,
                                         1 + field * nGuides)
    print(fiberID_range)
    outFileName = 'plPlugMapM-{0}{3}-{1}-{2:02d}_{4}.par'.format(
        plateID, mjd, fscanId, pointingName, colourDict[field])

    # Gets a list of the holes that we should keep for this fscanID
    validHoles = plPlugMapObj[(plPlugMapObj['holeType'] == 'LIGHT_TRAP') |
                              (np.in1d(plPlugMapObj['fiberId'],
                                       fiberID_range))]

    lightTraps = validHoles[validHoles['holeType'] == 'LIGHT_TRAP']
    guides = validHoles[validHoles['holeType'] == 'GUIDE']
    alignments = validHoles[validHoles['holeType'] == 'ALIGNMENT']

    guides = guides[lookupArray[:, 1] - 1]
    guides['fiberId'] = lookupArray[:, 0]

    sortedPlPlugMapM = np.concatenate((lightTraps, alignments, guides))

    enums = {'holeType': ['HOLETYPE', yannyFile._enum_cache['HOLETYPE']],
             'objType': ['OBJTYPE', yannyFile._enum_cache['OBJTYPE']]}
    yanny.write_ndarray_to_yanny(outFileName, sortedPlPlugMapM, enums=enums,
                                 structname='PLUGMAPOBJ')

    # Replaces the guidenums with the slice used for this file
    for ii, line in enumerate(header):
        if line.startswith('guidenums' + str(pointingNum + 1)):
            guides = str('guidenums' + str(pointingNum + 1)) + ' ' + \
                ' '.join(map(str, np.arange(1, nGuides + 1)))
            header[ii] = guides
            break

    addHeader(header, outFileName, plateID, fscanId, field,
              mjd, colourDict[field])

    return


if __name__ == '__main__':

    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]))

    parser.add_argument('PLATEID', type=int, help='The plateID.')
    parser.add_argument('POINTING', type=str, help='The pointing.')
    parser.add_argument('FIELD', type=int, help='The field to map.')
    parser.add_argument('MJD', type=int, help='The MJD of the scan.')
    parser.add_argument('--lookupTable', '-l', metavar='lookupTable',
                        type=str, default=None,
                        help='The lookup table linking fibres and holes.')
    parser.add_argument('--fscanId', '-f', metavar='fscanId',
                        type=int, default=1, help='The fscanId to create.')

    args = parser.parse_args()

    create_plPlugMapM_LCO(args.PLATEID, args.POINTING, args.FIELD, args.MJD,
                          lookupTable=args.lookupTable, fscanId=args.fscanId)
