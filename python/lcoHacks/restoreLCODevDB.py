#!/usr/bin/env python
# encoding: utf-8
#
# restoreLCODevDB.py
#
# Created by José Sánchez-Gallego on 12 Jul 2016.


from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import subprocess
import os

from sdss.internal.database.connections import LCODatabaseDevAdminLocalConnection as db


apodb_path = '/data/apodb'
platedb_file = 'platedb_for_dev.sql'
catalogdb_file = 'catalogdb_for_dev.sql'


def restoreLCODevDB():
    """Restores lcodb_dev from a file and modifies it."""

    # First we drop lcodb_dev
    print('Dropping lcodb_dev ... ')
    dropdb = subprocess.Popen('dropdb -U postgres lcodb_dev', shell=True)
    dropdb.communicate()

    # Recreates the DB
    print('Creating lcodb_dev ... ')
    createdb = subprocess.Popen('createdb -T template0 -U postgres lcodb_dev',
                                shell=True)
    createdb.communicate()

    # Restores platedb and catalogdb
    for file in [platedb_file, catalogdb_file]:
        schema = file.split('_')[0]
        print('Restoring {0} ... '.format(schema))
        path = os.path.join(apodb_path, file)
        restoreSchema = subprocess.Popen(
            'pg_restore -d lcodb_dev -Fc -U sdssdb_admin {0}'.format(path),
            shell=True)
        restoreSchema.communicate()

    # Now that the DB exists, imports the model classes
    from sdss.internal.database.apo.platedb import ModelClasses as platedb

    session = db.Session()

    # Adds location "LCO Cosmic"
    print('Adding LCO Cosmic ...')
    with session.begin():
        newLocation = platedb.PlateLocation(label='LCO Cosmic')
        session.add(newLocation)

    # Changes the sign of the declination of all plates that are not at LCO
    print('Modifying declinations and location ...')

    plates = session.query(platedb.Plate).join(platedb.PlateLocation).filter(
        platedb.PlateLocation.label != 'LCO' and
        platedb.PlateLocation.label != 'LCO Cosmic').all()

    lco_location_pk = session.query(platedb.PlateLocation).filter(
        platedb.PlateLocation.label == 'LCO').first().pk
    lco_cosmic_location_pk = session.query(platedb.PlateLocation).filter(
        platedb.PlateLocation.label == 'LCO Cosmic').first().pk

    with session.begin():
        for plate in plates:
            if len(plate.plate_pointings) == 0:
                continue
            for ii in range(len(plate.plate_pointings)):
                plate.plate_pointings[ii].pointing.center_dec = (
                    float(plate.plate_pointings[ii].pointing.center_dec) * -1.)

            location = plate.location.label
            if location == 'APO':
                plate.plate_location_pk = lco_location_pk
            elif location == 'Cosmic':
                plate.plate_location_pk = lco_cosmic_location_pk

    print('Removing extraneous cartridges ... ')
    cartridges = session.query(platedb.Cartridge).all()
    with session.begin():
        for cart in cartridges:
            if cart.number <= 5:
                pass
                # cart.number += 20
            else:
                session.delete(cart)

    return


if __name__ == '__main__':
    restoreLCODevDB()
