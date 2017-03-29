#!/usr/bin/env python
# encoding: utf-8
#
# facke-guider.py
#
# Created by José Sánchez-Gallego on 29 Mar 2017.


from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import opscore
from opscore.protocols.parser import CommandParser
from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

from opscore.utility.sdss3logging import setupRootLogger, setConsoleLevel
import logging

import opscore.protocols.keys as keys
import opscore.protocols.validation as validation

import actorcore.CommandLinkManager as cmdLinkManager
import actorcore.Command as actorCmd
import actorcore.CmdrConnection

import ConfigParser
import Queue

from twisted.internet import reactor

import imp
import inspect
import os
import re
import sys
import threading
import traceback


class FakeGuider(object):

    def __init__(self, name, productName='guiderActor', makeCmdrConnection=True):

        self.name = name
        self.productName = productName if productName else self.name
        product_dir_name = '$%s_DIR' % (self.productName.upper())
        self.product_dir = os.path.expandvars(product_dir_name)

        self.parser = CommandParser()

        self.config = ConfigParser.ConfigParser()
        self.config.read(os.path.join(os.path.dirname(__file__), 'guider.cfg'))

        self.configureLogs()

        # The list of all connected sources.
        tronInterface = ''
        tronPort = 9994
        self.commandSources = cmdLinkManager.listen(self,
                                                    port=tronPort,
                                                    interface=tronInterface)
        # The Command which we send uncommanded output to.
        self.bcast = actorCmd.Command(self.commandSources,
                                      'self.0', 0, 0, None, immortal=True)

        # IDs to send commands to ourself.
        self.selfCID = self.commandSources.fetchCid()
        self.synthMID = 1

        # commandSets are the command handler packages. Each handles
        # a vocabulary, which it registers when loaded.
        # We gather them in one place mainly so that "meta-commands" (init, status)
        # can find the others.
        self.commandSets = {}

        self.handler = validation.CommandHandler()

        self.attachAllCmdSets()

        self.commandQueue = Queue.Queue()
        self.shuttingDown = False

        if makeCmdrConnection:
            self.cmdr = actorcore.CmdrConnection.Cmdr(name, self)
            self.cmdr.connectionMade = self._connectionMade
            self.cmdr.connect()
        else:
            self.cmdr = None

    def configureLogs(self, cmd=None):
        """ (re-)configure our logs. """

        self.logDir = self.config.get('logging', 'logdir')
        assert self.logDir, "logdir must be set!"

        # Make the root logger go to a rotating file. All others derive from this.
        setupRootLogger(self.logDir)

        # The real stderr/console filtering is actually done through the console Handler.
        try:
            consoleLevel = int(self.config.get('logging', 'consoleLevel'))
        except:
            consoleLevel = int(self.config.get('logging', 'baseLevel'))
        setConsoleLevel(consoleLevel)

        # self.console needs to be renamed ore deleted, I think.
        self.console = logging.getLogger('')
        self.console.setLevel(int(self.config.get('logging', 'baseLevel')))

        self.logger = logging.getLogger('actor')
        self.logger.setLevel(int(self.config.get('logging', 'baseLevel')))
        self.logger.propagate = True
        self.logger.info('(re-)configured root and actor logs')

        self.cmdLog = logging.getLogger('cmds')
        self.cmdLog.setLevel(int(self.config.get('logging', 'cmdLevel')))
        self.cmdLog.propagate = True
        self.cmdLog.info('(re-)configured cmds log')

        if cmd:
            cmd.inform('text="reconfigured logs"')

    def versionString(self, cmd):
        """ Return the version key value.

        If you simply want to generate the keyword, call .sendVersionKey().
        """

        cmd.warn("text='pathetic version string: unknown'")
        return 'unknown'

    def sendVersionKey(self, cmd):
        """ Generate the version keyword in response to cmd. """

        version = self.versionString(cmd)
        cmd.inform('version=%s' % (qstr(version)))

    def triggerHubConnection(self):
        """ Send the hub a command to connect back to us. """

        if not self.cmdr:
            self.bcast.warn('text="CANNOT ask hub to connect to us, '
                            'since we do not have a connection to it yet!"')
            return

        self.bcast.warn('%s is asking the hub to connect back to us' % (self.name))
        self.cmdr.dispatcher.executeCmd(opscore.actor.keyvar.CmdVar
                                        (actor='hub', cmdStr='startNubs %s' % (self.name),
                                         timeLim=5.0))

    def _connectionMade(self):
        """ twisted arranges to call this when self.cmdr has been established. """

        self.bcast.warn('%s is connected to the hub.' % (self.name))

        #
        # Request that tron connect to us.
        #
        self.triggerHubConnection()
        self.connectionMade()

    def connectionMade(self):
        """ For overriding. """
        pass

    def attachCmdSet(self, cname, path=None):
        """ (Re-)load and attach a named set of commands. """

        if path is None:
            path = [os.path.join(self.product_dir, 'python', self.productName, 'Commands')]

        self.logger.info("attaching command set %s from path %s", cname, path)

        file = None
        try:
            file, filename, description = imp.find_module(cname, path)
            self.logger.debug("command set file=%s filename=%s from path %s",
                              file, filename, path)
            mod = imp.load_module(cname, file, filename, description)
        except ImportError as e:
            raise RuntimeError('Import of %s failed: %s' % (cname, e))
        finally:
            if file:
                file.close()

        # Instantiate and save a new command handler.
        cmdSet = getattr(mod, cname)(self)

        # Check any new commands before finishing with the load. This
        # is a bit messy, as the commands might depend on a valid
        # keyword dictionary, which also comes from the module
        # file.
        #
        # BAD problem here: the Keys define a single namespace. We need
        # to check for conflicts and allow unloading. Right now we unilaterally
        # load the Keys and do not unload them if the validation fails.
        if hasattr(cmdSet, 'keys') and cmdSet.keys:
            keys.CmdKey.addKeys(cmdSet.keys)
        valCmds = []
        for v in cmdSet.vocab:
            try:
                verb, args, func = v
            except ValueError as e:
                raise RuntimeError("vocabulary word needs three parts: %s" % (v))

            # Check that the function exists and get its help.
            #
            funcDoc = inspect.getdoc(func)
            valCmd = validation.Cmd(verb, args, help=funcDoc) >> func
            valCmds.append(valCmd)

        # Got this far? Commit. Save the Cmds so that we can delete them later.
        oldCmdSet = self.commandSets.get(cname, None)
        cmdSet.validatedCmds = valCmds
        self.commandSets[cname] = cmdSet

        # Delete previous set of consumers for this named CmdSet, add new ones.
        if oldCmdSet:
            self.handler.removeConsumers(*oldCmdSet.validatedCmds)
        self.handler.addConsumers(*cmdSet.validatedCmds)

        self.logger.debug("handler verbs: %s" % (self.handler.consumers.keys()))

    def attachAllCmdSets(self, path=None):
        """ (Re-)load all command classes -- files in ./Command which end with Cmd.py.
        """

        if path is None:
            self.attachAllCmdSets(path=os.path.join(os.path.expandvars('$ACTORCORE_DIR'),
                                                    'python', 'actorcore', 'Commands'))
            self.attachAllCmdSets(path=os.path.join(self.product_dir, 'python', self.productName,
                                                    'Commands'))
            return

        dirlist = os.listdir(path)
        dirlist.sort()
        self.logger.info("loading %s" % (dirlist))

        for f in dirlist:
            if os.path.isdir(f) and not f.startswith('.'):
                self.attachAllCmdSets(path=f)
            if re.match('^[a-zA-Z][a-zA-Z0-9_-]*Cmd\.py$', f):
                self.attachCmdSet(f[:-3], [path])

    def cmdTraceback(self, e):
        eType, eValue, eTraceback = sys.exc_info()
        tbList = traceback.extract_tb(eTraceback)
        where = tbList[-1]

        return "%r at %s:%d" % (eValue, where[0], where[1])

    def runActorCmd(self, cmd):
        try:
            cmdStr = cmd.rawCmd
            self.cmdLog.debug('raw cmd: %s' % (cmdStr))

            try:
                validatedCmd, cmdFuncs = self.handler.match(cmdStr)
            except Exception as e:
                cmd.fail('text=%s' % (qstr("Unmatched command: %s (exception: %s)" %
                                           (cmdStr, e))))
                # tback('actor_loop', e)
                return

            if not validatedCmd:
                cmd.fail('text=%s' % (qstr("Unrecognized command: %s" % (cmdStr))))
                return

            self.cmdLog.info('< %s:%d %s' % (cmd.cmdr, cmd.mid, validatedCmd))
            if len(cmdFuncs) > 1:
                cmd.warn('text=%s' % (qstr("command has more than one callback (%s): %s" %
                                           (cmdFuncs, validatedCmd))))
            try:
                cmd.cmd = validatedCmd
                for func in cmdFuncs:
                    func(cmd)
            except Exception as e:
                oneLiner = self.cmdTraceback(e)
                cmd.fail('text=%s' % (qstr("command failed: %s" % (oneLiner))))
                # tback('newCmd', e)
                return

        except Exception as e:
            cmd.fail('text=%s' %
                     (qstr('completely unexpected exception when processing a new command: %s'
                      % (e))))
            try:
                tback('newCmdFail', e)
            except:
                pass

    def actor_loop(self):
        """ Check the command queue and dispatch commands."""

        while True:
            try:
                cmd = self.commandQueue.get(block=True, timeout=3)
            except Queue.Empty:
                if self.shuttingDown:
                    return
                else:
                    continue
            self.runActorCmd(cmd)

    def commandFailed(self, cmd):
        """ Gets called when a command has failed. """
        pass

    def newCmd(self, cmd):
        """ Dispatch a newly received command. """

        self.cmdLog.info('new cmd: %s' % (cmd))

        # Empty cmds are OK; send an empty response...
        if len(cmd.rawCmd) == 0:
            cmd.finish('')
            return None

        if self.runInReactorThread:
            self.runActorCmd(cmd)
        else:
            self.commandQueue.put(cmd)

        return self

    def callCommand(self, cmdStr):
        """ Send ourselves a command. """

        cmd = actorCmd.Command(self.commandSources, 'self.%d' % (self.selfCID),
                               cid=self.selfCID, mid=self.synthMID, rawCmd=cmdStr)
        self.synthMID += 1
        self.newCmd(cmd)

    def _shutdown(self):
        self.shuttingDown = True

    def run(self, doReactor=True):
        """ Actually run the twisted reactor. """
        try:
            self.runInReactorThread = self.config.getboolean(self.name, 'runInReactorThread')
        except:
            self.runInReactorThread = False

        self.logger.info("starting reactor (in own thread=%s)...." % (not self.runInReactorThread))
        try:
            if not self.runInReactorThread:
                threading.Thread(target=self.actor_loop).start()
            if doReactor:
                reactor.run()
        except Exception as e:
            tback('run', e)

        if doReactor:
            self.logger.info("reactor dead, cleaning up...")
            self._shutdown()

    def output_file(self):
        self.bcast.inform('text="THIS IS A VERY LOUD TEST"')
        reactor.callLater(5, self.output_file)


if __name__ == '__main__':
    guider = FakeGuider('guider')
    guider.run(startThreads=False)
    guider.output_file()
