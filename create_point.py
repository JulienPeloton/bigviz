#
# Copyright 2018 Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import division, absolute_import, print_function
from astropy.io import fits
import numpy as np
import sys

import argparse

def addargs(parser):
    """ Parse command line arguments """

    ## Number of row
    parser.add_argument(
        '-npoints', dest='npoints',
        required=True,
        type=int,
        help='Number of points. It will be rounded to the closest cubic power')

    ## Output file name
    parser.add_argument(
        '-filename', dest='filename',
        default='test_file.fits',
        help='Name of the output file with .fits extension')


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="""
    Create dummy FITS file for test purpose.
    To create a FITS file just run
        `python create_point.py -npoints <> -filename <>`
    or
        `python create_point.py -h`
    to get help.
            """)
    addargs(parser)
    args = parser.parse_args(None)

    ## Grab the number of row desired
    n = int(args.npoints **(1./3))

    ## Make a 3D mesh
    x = np.arange(n)
    y = np.arange(n)
    z = np.arange(n)
    mesh = np.meshgrid(x, y, z)

    ## Primary HDU - just a header
    hdr = fits.Header()
    hdr['OBSERVER'] = "Toto l'asticot"
    hdr['COMMENT'] = "Here's some commentary about this FITS file."
    primary_hdu = fits.PrimaryHDU(header=hdr)

    a1 = np.array(mesh[0].flatten(), dtype=np.float64)
    a2 = np.array(mesh[1].flatten(), dtype=np.float64)
    a3 = np.array(mesh[2].flatten(), dtype=np.float64)
    names = ["x", "y", "z"]

    ## Create each column
    col1 = fits.Column(name=names[0], format='E', array=a1)
    col2 = fits.Column(name=names[1], format='E', array=a2)
    col3 = fits.Column(name=names[2], format='E', array=a3)

    ## Format into columns
    cols = fits.ColDefs([col1, col2, col3])

    ## Make the first HDU.
    hdu1 = fits.BinTableHDU.from_columns(cols)

    ## Concatenate all HDU
    hdul = fits.HDUList([primary_hdu, hdu1])

    ## Save on disk
    hdul.writeto(args.filename)
