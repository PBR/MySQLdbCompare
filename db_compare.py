#!/usr/bin/python
# -*- coding: utf-8 -*-

""" This script checks compares two databases schemas, first display
which tables are only present in the reference schema.
Then for each tables which are in the reference database it displays if
the attributes and (primary and foreign) which are differents.
"""

import sys
import logging
import getpass


try:
    import argparse
except ImportError:
    print "You need to install python-argparse"
    sys.exit(1)


try:
    from sqlalchemy import create_engine
    from sqlalchemy.engine import reflection
    from  sqlalchemy.exc import OperationalError
except ImportError:
    print "You need to install python-sqlalchemy"
    sys.exit(2)


# Initial simple logging stuff
logging.basicConfig()
LOG = logging.getLogger("pkgdb")
if '--debug' in sys.argv:
    LOG.setLevel(logging.DEBUG)
elif '--verbose' in sys.argv:
    LOG.setLevel(logging.INFO)


bold = "\033[1m"
red = "\033[0;31m"
reset = "\033[0;0m"

if '--nocolor' in sys.argv:
    bold = ''
    red = ''
    reset = ''

class PasswordInputError(Exception):
    '''The password enter is None or empty.
    '''
    pass


def setup_parser():
    '''Set the main arguments.
    '''
    pars = argparse.ArgumentParser()
    # General connection options
    pars.add_argument('dbname', help="Name of the reference database")
    pars.add_argument('dbname2', help="Name of the database to compare")
    pars.add_argument('dbuser',
        help="User to access the reference database")
    pars.add_argument('--dbuser2', default=None,
        help="User to access the database to compare" \
            " (if null, dbuser will be used")
    pars.add_argument('--dbpassword', default=None,
        help="Password of the reference database " \
            "(can also be given later)")
    pars.add_argument('--dbpassword2', default=None,
        help="Passwrod of the database to compare " \
            "(can also be given later)")
    pars.add_argument('--host', default='localhost',
                help="Host of the database to query " \
                    "(default: localhost)")
    pars.add_argument('--port', default='3306',
                help="Port of the database to query (default: 3306)")
    pars.add_argument('--text', action='store_false',
                help="Do not color the output")
    pars.add_argument('--verbose', action='store_true',
                help="give more info about what's going on")
    pars.add_argument('--debug', action='store_true',
                help="output bunches of debugging info")
    pars.add_argument('--nocolor', action='store_true',
                help="Remove colors from output")
    return pars


def get_engine_db(name, user, passwd, host="localhost", port="3306"):
    ''' Generate the database engine for a given database using the
    given user and password on the given host and port.

    :arg name the database name
    :arg user the user to log in the database
    :arg passwd the password to log in the database
    :arg host the host where is the database located
    :arg port the port to access the databas
    '''
    if port is not None:
        return create_engine('mysql://%s:%s@%s:%s/%s' % (
            user, passwd, host, port, name))
    else:
        return create_engine('mysql://%s:%s@%s/%s' % (
            user, passwd, host, name))


def main(nameref, namecomp, userref, usercomp,
        passref=None, passcomp=None, host="localhost", port="3306",
        color=True):
    ''' Main function.
    Compare a reference database to another one:
    - Compare the list of tables in the reference database and print
    those which are not present in the second database
    - Compare each fields in each database and print structural
    differences between the reference database and the compared one
    - Compare the primary and foreign key of each table in the database
    and print the keys having differences

    :arg nameref the name of the reference database
    :arg namecomp the name of the database to compare to the reference
    :arg userref the user to log in the reference database
    :arg usercomp the user to log in the database to compare
    :arg passred the password for the user to log in the reference
    database
    :arg passcomp the password for the user to log in the database to
    compare
    :arg host host of the database (both databases should be on the same
    machine)
    :arg port port of the database (both databases should be on the same
    machine)
    :arg color wether to color the output or not
    '''

    LOG.info("Starting comparison")

    if usercomp is None:
        usercomp = userref
        LOG.debug("Using same username for both database")

    if passref is None:
        passref = getpass.getpass("Password, reference database: ")
    if passref is None or passref == "":
        LOG.info("No (or empty) password given, exiting")
        raise PasswordInputError("Password error",
                    "No (or empty) password given, exiting")
    LOG.info("Establishing connection with %s" % nameref)
    engineref = get_engine_db(nameref, userref, passref, host, port)

    inspref = reflection.Inspector.from_engine(engineref)
    tablesref = inspref.get_table_names()

    LOG.info("Establishing connection with %s" % namecomp)
    try:
        enginecomp = get_engine_db(namecomp, usercomp, passref, host,
                                    port)
    except OperationalError:
        if passcomp is None:
            passcomp = getpass.getpass("Password, comparison database: ")
        enginecomp = get_engine_db(namecomp, usercomp, passcomp, host,
                                    port)

    inspcomp = reflection.Inspector.from_engine(enginecomp)
    tablescomp = inspcomp.get_table_names()

    LOG.info("Compare table between %s and %s" % (nameref, namecomp))

    print "Table present in %s and not in %s" % (nameref, namecomp)
    tables = set(tablesref).difference(set(tablescomp))
    for table in tables:
        if color:
            print red, bold, table, reset
        else:
            print " ", table
    print "Total: %s tables" % len(tables)

    LOG.info("Compare attributes of the tables between %s and %s"
                % (nameref, namecomp))

    print "\nAttributes of table differing between %s and %s" % \
        (nameref, namecomp)
    for table in list(set(tablesref).intersection(set(tablescomp))):
        if color:
            print red, bold, table, reset
        else:
            print " ", table
        LOG.debug("Table: %s " % table)
        # Retrieve all the columns
        colref = inspref.get_columns(table)
        colcomp = inspcomp.get_columns(table)

        for col in colref:
            flag = False
            name = col['name']
            LOG.debug("  Column : %s " % name)
            LOG.debug("  Row    : %s" % col)
            for colc in colcomp:
                if colc['name'] == name:
                    for key in col.keys():
                        if key in colc.keys() and \
                            str(col[key]) != str(colc[key]):
                            LOG.debug("   content: %s - %s = %s " % (
                                col[key], colc[key],
                                col[key] == colc[key]))
                            flag = True
                            out = "     %s \t %s -> %s" % (key, col[key],
                            colc[key])
            if flag is True:
                print "   ", col['name']
                print out

        # Retrieve all the primary key
        LOG.info("Primary keys")
        pkref = inspref.get_pk_constraint(table)
        keyref = pkref["constrained_columns"]
        LOG.debug("ref : %s" % keyref)
        pkcomp = inspcomp.get_pk_constraint(table)
        keycomp = pkcomp["constrained_columns"]
        LOG.debug("comp : %s" % keycomp)
        diff = set(keyref).symmetric_difference(set(keycomp))
        if len(diff) > 0:
            print "    Primary key differing: %s" % \
                        ", ".join(diff)

        # Retrieve all the foreign key
        LOG.info("Foreign keys")
        fkref = inspref.get_foreign_keys(table)
        fkcomp = inspcomp.get_foreign_keys(table)
        keyref = ["+".join(key['referred_columns']) for key in fkref]
        LOG.debug("ref : %s" % keyref)
        keycomp = ["+".join(key['referred_columns']) for key in fkcomp]
        LOG.debug("comp : %s" % keycomp)
        diff = set(keyref).symmetric_difference(set(keycomp))
        if len(diff) > 0:
            print "    Referential contrains differing: %s" % \
                        ", ".join(diff)


if __name__ == "__main__":
    try:
        parser = setup_parser()
        args = parser.parse_args()
        main(args.dbname, args.dbname2,
            args.dbuser, args.dbuser2,
            host=args.host, color=args.text)
    except KeyboardInterrupt:
        print "\nInterrupted by user."
        sys.exit(3)
    except OperationalError, e:
        print "\nDatabase error: %s" % e
        sys.exit(4)
    except PasswordInputError, e:
        print '\n%s: %s' % (e[0], e[1])
        sys.exit(5)
    except Exception, e:
        print '\nERROR: %s' % (e)
        sys.exit(6)
