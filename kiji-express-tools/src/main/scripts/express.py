#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

# --------------------------------------------------------------------------------------------------

# (c) Copyright 2014 WibiData, Inc.
#
# See the NOTICE file distributed with this work for additional
# information regarding copyright ownership.
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

# --------------------------------------------------------------------------------------------------

# The express script provides tools for running Express jobs and interacting with KijiExpress.
# Usage:
#
# bash> $EXPRESS_HOME/bin/express.py <command-name> [--flag=value] args...
#
# For full usage information, use:
#
# bash> $EXPRESS_HOME/bin/express.py help


import hashlib
import argparse
import glob
import itertools
import logging
import os
import re
import subprocess
import sys
import time


EXPRESS_TOOL = "org.kiji.express.flow.ExpressTool"
TMP_JARS_TOOL = "org.kiji.express.tool.TmpJarsTool"

EXPRESS_HOME = "EXPRESS_HOME"
HADOOP_HOME = "HADOOP_HOME"
HBASE_HOME = "HBASE_HOME"
KIJI_HOME = "KIJI_HOME"
SCHEMA_SHELL_HOME = "SCHEMA_SHELL_HOME"

KIJI_CLASSPATH = "KIJI_CLASSPATH"



class Error(Exception):
  """Errors used in this module."""
  pass


# --------------------------------------------------------------------------------------------------
# Utilities


def md5_sum(file_path):
    """Computes the MD5 sum of a file.

    Args:
      file_path: Path of the file to compute the MD5 sum for.
    Returns:
      The file MD5 sum, represented as an hex string (32 characters).
    """
    md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        md5.update(f.read())
    return md5.hexdigest()


def expand_classpath_entry(entry):
    """Expand the specified classpath entry if it contains a wildcard.

    Expand the '*' classpath wildcard by applying the glob '*.jar'.

    Yields:
      Expanded classpath entries.
    """
    if os.path.basename(entry) != "*":
        yield entry
    else:
        expanded = glob.glob(os.path.join(os.path.dirname(entry), "*.jar"))
        expanded = sorted(expanded)
        yield from expanded


def flat_map(operator, iterable):
    """Concatenates the collections produced by an operator mapped on a given collection.

    Args:
      operator: Operator to apply on each input element from iterable.
          The expected signature for operator is: element -> iterable.
      iterable: Iterable of elements to apply the operator onto.
    Returns:
      An iterable of the concatenation of the resulting collections.
    """
    return itertools.chain.from_iterable(map(operator, iterable))


def unique(iterable, key=None):
    """Removes duplicate items from the specified iterable."""
    watched = set()
    if key is None:
        key = lambda x: x  # identity

    def watch(item):
        """Stateful filter that remembers items previously watched."""
        item_key = key(item)
        if item_key in watched:
            return False
        else:
            watched.add(item_key)
            return True

    return filter(watch, iterable)


def tab_indent(text):
    """Left-indents a string of text."""
    return "\t" + text


def _exists_or_log(entry):
    exist = os.path.exists(entry)
    if not exist:
        logging.warning("Classpath entry does not exist: %r", entry)
    return exist


def normalize_classpath(classpath):
    """Normalizes the given classpath entries.

    Performs the following normalizations:
     - Classpath wildcards are expanded.
     - Symlinks are expanded.
     - Paths are made absolute.
     - Duplicate paths are eliminated.
     - Non-existent paths are removed.

    Args:
      classpath: Iterable of classpath entries.
    Returns:
      Iterable of normalized classpath entries.
    """
    classpath = flat_map(expand_classpath_entry, classpath)
    classpath = filter(_exists_or_log, classpath)
    classpath = map(os.path.realpath, classpath)
    classpath = unique(classpath)
    # classpath = unique(classpath, key=os.path.basename)
    classpath = unique(classpath, key=md5_sum)
    return classpath


# --------------------------------------------------------------------------------------------------


class HomedTool(object):
    """Wraps an installation configured through a X_HOME environment variable.

    This assumes the installation provides a tool under "${X_HOME}/bin/<tool>".
    """

    def __init__(self, env=os.environ):
        self._env = env
        assert (self.home_env_key in self._env), \
            ("Environment variable undefined: %r" % self.home_env_key)
        self._home_dir = os.path.abspath(self._env[self.home_env_key])
        assert os.path.isdir(self.home_dir), ("Home directory not found: %r" % self.home_dir)

    @property
    def home_dir(self):
        return self._home_dir

    @property
    def tool_path(self):
        tool_path = os.path.join(self.home_dir, "bin", self.tool)
        assert os.path.isfile(tool_path), ("Command-line tool not found: %r" % tool_path)
        return tool_path

    def _acquire_classpath(self):
        stdout = subprocess.check_output([self.tool_path, "classpath"], universal_newlines=True)
        stdout = stdout.strip()
        classpath = stdout.split(":")
        classpath = filter(None, classpath)
        classpath = tuple(classpath)
        logging.debug("%r reported the following classpath:\n%s",
                      self.tool_path, "\n".join(map(tab_indent, classpath)))
        return classpath

    @property
    def classpath(self):
        """Reports the runtime classpath for this homed tool installation.

        Returns:
          A tuple of classpath entries for this installation.
        """
        if not hasattr(self, "_classpath"):
            self._classpath = tuple(normalize_classpath(self._acquire_classpath()))
        return self._classpath


class HadoopTool(HomedTool):
    _RE_HADOOP_VERSION = re.compile(r"^Hadoop (.*)$")

    @property
    def home_env_key(self):
        return "HADOOP_HOME"

    @property
    def tool(self):
        return "hadoop"

    def _acquire_version(self):
        stdout = subprocess.check_output([self.tool_path, "version"], universal_newlines=True)
        stdout = stdout.strip()
        lines = stdout.splitlines()
        top_line = lines[0]
        match = self._RE_HADOOP_VERSION.match(top_line)
        assert (match is not None), ("Invalid output from command 'hadoop version': %r" % stdout)
        return match.group(1)

    @property
    def version(self):
        """Returns: the version ID of this Hadoop installation (eg. '2.0.0-mr1-cdh4.3.0')."""
        if not hasattr(self, '_version'):
            self._version = self._acquire_version()
        return self._version

    @property
    def major_version(self):
        """Returns: the major version of this Hadoop installation (eg. 1 or 2)."""
        return self.version.split('.')[0]  # Pick major version


class HBaseTool(HomedTool):
    @property
    def home_env_key(self):
        return "HBASE_HOME"

    @property
    def tool(self):
        return "hbase"


class KijiTool(HomedTool):
    @property
    def home_env_key(self):
        return "KIJI_HOME"

    @property
    def tool(self):
        return "kiji"


# --------------------------------------------------------------------------------------------------


def list_libdir_jars(home_env_key=None, home=None, lib=None):
    """Lists the JAR files in the specified lib/ directory.

    Exactly one of home_env_key, home or lib must be specified.

    Args:
      home_env_key: Optional environment variable defining the home directory.
      home: Optional home directory path.
      lib: Optional lib directory path.

    Yields:
      The classpath entries from the specified lib directory.
    """
    if lib is None:
        if home is None:
            home = os.environ.get(home_env_key)
            assert (home is not None), ("Environment variable undefined: %r" % home_env_key)
        else:
            assert (home_env_key is None)
        lib = os.path.join(home, 'lib')
    else:
        assert (home is None) and (home_env_key is None)

    # Classpath entries named '*' match the glob '*.jar':
    return glob.glob(os.path.join(lib, '*.jar'))


# TODO: Filter out duplicated slf4j jars.

# FIXME: how does one get 'hadoop2-hbase96' ?
# dist_name = 'hadoop%s' % hadoop.major_version

# --------------------------------------------------------------------------------------------------


KIJI_CLASSPATH = "KIJI_CLASSPATH"


class ExpressTool(object):
    def __init__(self, env=os.environ):
        self._env = env
        assert (self.home_env_key in self._env), \
            ("Environment variable undefined: %r" % self.home_env_key)
        self._home_dir = os.path.abspath(self._env[self.home_env_key])
        assert os.path.isdir(self.home_dir), ("Home directory not found: %r" % self.home_dir)

        self._hadoop = HadoopTool(env=self._env)
        self._hbase = HBaseTool(env=self._env)
        self._kiji = KijiTool(env=self._env)

    @property
    def home_env_key(self):
        return "EXPRESS_HOME"

    @property
    def home_dir(self):
        return self._home_dir

    @property
    def hadoop(self):
        return self._hadoop

    @property
    def hbase(self):
        return self._hbase

    @property
    def kiji(self):
        return self._kiji

    def _list_classpath_entries(self):
        # TODO: include --libjars

        if KIJI_CLASSPATH in self._env:
            user_classpath = self._env[KIJI_CLASPATH].split(":")
            yield from user_classpath

        yield os.path.join(self.home_dir, "conf")
        yield from list_libdir_jars(home=self.home_dir)
        yield from self.kiji.classpath
        yield from list_libdir_jars(home_env_key="SCHEMA_SHELL_HOME")
        yield from list_libdir_jars(home_env_key="MODELING_HOME")

    def get_classpath(self, lib_jars=()):
        """Reports the Express classpath.

        Args:
          lib_jars: Optional collection of user-specified JARs to include.
        Returns:
          An iterable of classpath entries.
        """
        express_classpath = self._list_classpath_entries()
        classpath = itertools.chain(lib_jars, express_classpath)
        return normalize_classpath(classpath)

    def list_paths_for_dist_cache(self, lib_jars):
        """Lists the JAR files to send to the distributed cache.

        Args:
          lib_jars: Collection of JAR files to prepare for an Express job.
        Returns:
          Iterable of paths to send to the distributed cache.
        """
        express_classpath = ":".join(self.get_classpath())
        cmd = ["java", "-classpath", express_classpath, TMP_JARS_TOOL, ":".join(lib_jars)]
        logging.debug("Running command:\n%s\n", " \\\n\t".join(map(repr, cmd)))
        output = subprocess.check_output(cmd, universal_newlines=True).strip()
        jars = output.split(",")
        jars = sorted(jars)
        logging.debug("JARs sent to the distributed cache:\n%s", "\n".join(map(tab_indent, jars)))
        return jars


# --------------------------------------------------------------------------------------------------


# Usage string for script, used by the help command.
USAGE = """\
Usage:
  %(this)s [--do]=command [--flags ...] arguments...

Commands:
  help            - Displays this help message.
  shell           - Launches interactive KijiExpress shell.
  schema-shell    - Starts KijiSchema Shell loaded with KijiExpress extensions.
  job             - Java exec a compiled KijiExpress job.
  jar             - Runs arbitrary Scala or Java program with express classpath.
  classpath       - Prints the classpath used to run KijiExpress.

Environment Variables:
  EXPRESS_JAVA_OPTS  - Extra arguments to pass to the KijiExpress's JVM.
  KIJI_CLASSPATH     - Colon-separated jars for classpath and distributed cache.
  JAVA_LIBRARY_PATH  - Colon-separated paths to additional native libs.
  JAVA_OPTS          - Java args to append to java command and sent to JVM.
"""

# --------------------------------------------------------------------------------------------------


_LOGGING_INITIALIZED = False


def parse_log_level(level):
  """Parses a logging level command-line flag.

  Args:
    level: Logging level command-line flag (string).
  Returns:
    Logging level (integer).
  """
  log_level = getattr(logging, level.upper(), None)
  if type(log_level) == int:
    return log_level

  try:
    return int(level)
  except ValueError:
    raise Error('Invalid logging-level: %r' % level)


def setup_logging(log_level):
  """Initializes the logging system.

  Args:
    log_level: Logging level.
  """
  global _LOGGING_INITIALIZED
  if _LOGGING_INITIALIZED:
    logging.debug('setup_logging: logging system already initialized')
    return

  log_formatter = logging.Formatter(
      fmt='%(asctime)s %(levelname)s %(filename)s:%(lineno)s : %(message)s',
  )

  # Override the log date formatter to include the time zone:
  def format_time(record, datefmt=None):
    time_tuple = time.localtime(record.created)
    tz_name = time.tzname[time_tuple.tm_isdst]
    return '%(date_time)s-%(millis)03d-%(tz_name)s' % dict(
        date_time=time.strftime('%Y%m%d-%H%M%S', time_tuple),
        millis=record.msecs,
        tz_name=tz_name,
    )
  log_formatter.formatTime = format_time

  logging.root.handlers.clear()
  logging.root.setLevel(log_level)

  console_handler = logging.StreamHandler()
  console_handler.setFormatter(log_formatter)
  console_handler.setLevel(log_level)
  logging.root.addHandler(console_handler)

  _LOGGING_INITIALIZED = True


# --------------------------------------------------------------------------------------------------


def make_arg_parser():
    parser = argparse.ArgumentParser(
        description="KijiExpress command-line interface."
    )
    parser.add_argument('--log-level', default="info", help='Logging level.')

    subparsers = parser.add_subparsers(title='action', dest='action')
    classpath_parser = subparsers.add_parser("classpath")
    jar_parser = subparsers.add_parser("jar")
    job_parser = subparsers.add_parser("job")
    shell_parser = subparsers.add_parser("shell")
    schema_shell_parser = subparsers.add_parser("schema-shell")

    # Flags specific to a subset of the commands:
    for subparser in (classpath_parser, jar_parser, job_parser, shell_parser):
        subparser.add_argument(
            '--jars',
            nargs='*',
            help='List of JAR files to place on the classpath and the distributed cache.',
        )
    for subparser in (job_parser, shell_parser):
        subparser.add_argument(
            '--mode',
            default='local',
            help=('Express mode: local or hdfs.'),
        )

    # Flags specific to the 'jar' command:
    jar_parser.add_argument(
        '--class',
        help='Class name of the Express job to run.',
    )
    jar_parser.add_argument(
        '--java-opts',
        nargs='*',
        help=('Optional list of options for the JVM '
              '(eg. --java-opts -Xmx2G -Xms1G -Dprop=val ...).'),
    )

    # Flags specific to the 'job' command:
    job_parser.add_argument(
        '--class',
        help='Class name of the Express job to run.',
    )
    job_parser.add_argument(
        '--hadoop-opts',
        nargs='*',
        default=list(),
        help=('Optional list of options for the Hadoop tool '
              '(eg. --hadoop-opts -conf <conf> -Dfs.defaultFS=...).'),
    )
    job_parser.add_argument(
        '--java-opts',
        nargs='*',
        help=('Optional list of options for the JVM '
              '(eg. --java-opts -Xmx2G -Xms1G -Dprop=val ...).'),
    )
    job_parser.add_argument(
        '--disable-user-jars-take-precedence',
        type=bool,
        help=('Whether user JARs take precedence or not in M/R task classpaths.'),
    )

    return parser

# --------------------------------------------------------------------------------------------------


class ExpressCLI(object):
    def __init__(self, flags, args, env=os.environ):
        self._flags = flags
        self._args = args
        self._env = env
        self._express = ExpressTool(env=self._env)

    @property
    def flags(self):
        return self._flags

    @property
    def args(self):
        return self._args

    @property
    def env(self):
        return self._env

    @property
    def express(self):
        return self._express

    def pop_args_head(self):
        head, self._args = self._args[0], self._args[1:]
        return head

    def classpath(self):
        assert (len(self.args) == 0), ('Unexpected command-line arguments: %r' % self.args)
        lib_jars = []
        if self.flags.jars is not None:
            lib_jars.extend(self.flags.jars.split(","))
        print(":".join(self.express.get_classpath(lib_jars=lib_jars)))
        return os.EX_OK

    def jar(self):
        class_name = getattr(self.flags, 'class')
        if (class_name is None) and (len(self.args) > 0):
            class_name = self.pop_args_head()
        assert (class_name is not None), ('No class name specified with [--class=]<class>.')

        classpath = []
        if self.flags.jars is not None:
            classpath.extend(self.flags.jars)
        classpath.extend(self.express.get_classpath())

        java_opts = []
        if self.flags.java_opts is not None:
            java_opts = [self.flags.java_opts]

        user_args = list(self.args)

        cmd = [
            "java",
            # This property is only needed in kiji-schema v1.1 :
            "-Dorg.kiji.schema.impl.AvroCellEncoder.SCHEMA_VALIDATION=DISABLED",
        ] + java_opts + [
            "-classpath", ":".join(classpath),
            class_name,
        ] + user_args

        logging.info("Running command:\n%s\n", " \\\n\t".join(map(repr, cmd)))
        return subprocess.call(cmd)

    @staticmethod
    def hadoop_native_libs(env):
        """Check for native libraries provided with hadoop distribution.

        Returns:
          A string that is the path to the native libraries.
        """
        java_library_path = list(filter(None, env.get("JAVA_LIBRARY_PATH", "").split(",")))

        hadoop = HadoopTool(env=env)
        hadoop_native_dir_path = os.path.join(env["HADOOP_HOME"], "lib", "native")
        if os.path.isdir(hadoop_native_dir_path):
            java_library_path.append(hadoop_native_dir_path)

            # Hadoop wants a certain platform version, then we hope to use it
            cmd = [
                "java",
                "-cp",
                ":".join(hadoop.classpath),
                "-Xmx32m",
                "org.apache.hadoop.util.PlatformName"
            ]
            output = subprocess.check_output(cmd, universal_newlines=True)
            java_platform = output.strip()
            logging.info('Using Hadoop platform: %r', java_platform)
            native_dirs = os.path.join(hadoop_native_dir_path, java_platform.replace(" ", "_"))
            if os.path.isdir(native_dirs):
                java_library_path.append(native_dirs)

        return java_library_path

    def job(self):
        class_name = getattr(self.flags, 'class')
        if (class_name is None) and (len(self.args) > 0):
            class_name = self.pop_args_head()
        assert (class_name is not None), \
            "Express job class unset, please use [--class=]<class>."

        # any more args must be user specified args for the express job
        job_args = list(self.args)
        logging.debug("Running Express job %r with parameters: %r", class_name, job_args)

        lib_jars = []
        if self.flags.jars is not None:
            lib_jars.extend(self.flags.jars)

        classpath = self.express.get_classpath(lib_jars=lib_jars)

        # ----------------------------------------
        # Java options:
        java_opts = list()

        # FIXME: Splitting on space is very brittle
        java_opts.extend(filter(None, self.env.get("EXPRESS_JAVA_OPTS", "").split(" ")))
        java_opts.extend(filter(None, self.env.get("JAVA_OPTS", "").split(" ")))

        # Workaround for OS X: a bug in JRE 1.6 creates "SCDynamicStore" errors.
        if self.env.get("uname") == "Darwin":  # FIXME!!!
            java_opts.append("-Djava.security.krb5.realm=")
            java_opts.append("-Djava.security.krb5.kdc=")

        lib_path = self.env.get("JAVA_LIBRARY_PATH", "").split(':')
        native_lib_path = self.hadoop_native_libs(self.env)
        if len(native_lib_path) > 0:
            lib_path.append(native_lib_path)
        lib_path = list(filter(None, map(str.strip, lib_path)))
        if len(lib_path) > 0:
            java_opts.append("-Djava.library.path=%s" % ':'.join(lib_path))

        logging.debug("Using JVM options: %r", java_opts)

        # ----------------------------------------
        # Hadoop generic options:
        hadoop_opts = list()
        if self.flags.disable_user_jars_take_precedence:
            hadoop_opts.append("-Dmapreduce.task.classpath.user.precedence=false")
        else:
            hadoop_opts.append("-Dmapreduce.task.classpath.user.precedence=true")
        hadoop_opts.extend(list(self.flags.hadoop_opts))
        dist_cache_jars = self.express.list_paths_for_dist_cache(lib_jars=lib_jars)
        hadoop_opts.append("-Dtmpjars=%s" % ",".join(dist_cache_jars))
        logging.debug("Hadoop generic options:\n%s", " \n".join(map(tab_indent, hadoop_opts)))

        # ----------------------------------------

        assert (self.flags.mode in ('local', 'hdfs')), \
            ('Invalid Express mode: %r' % self.flags.mode)
        run_mode = ('--%s' % self.flags.mode)

        cmd = [
            "java",
            # Property needed for kiji-schema v1.1:
            "-Dorg.kiji.schema.impl.AvroCellEncoder.SCHEMA_VALIDATION=DISABLED",
            "-classpath", ":".join(classpath),
        ] + list(filter(None, map(str.strip, java_opts))) + [
            EXPRESS_TOOL
        ] + hadoop_opts + [
            class_name,
            run_mode,
        ] + job_args

        logging.debug("Running command:\n%s", " \\\n\t".join(map(repr, cmd)))
        proc = subprocess.call(cmd)

    _MODE_SCRIPT = {
        "hdfs": "hdfs-mode.scala",
        "local": "local-mode.scala",
    }

    def shell(self):
        lib_jars = []
        if self.flags.jars is not None:
            lib_jars.extend(self.flags.jars.split(","))

        env = dict(self.env)

        express_cp = self.express.get_classpath(lib_jars=lib_jars)
        env["EXPRESS_CP"] = ":".join(express_cp)

        dist_cache_paths = self.express.list_paths_for_dist_cache(lib_jars=lib_jars)
        env["TMPJARS"] = ",".join(dist_cache_paths)

        # EXPRESS_MODE environment variable must be a path to the mode specific scala script:
        script_name = self._MODE_SCRIPT.get(self.flags.mode)
        assert (script_name is not None), ("Invalid Express mode: %r." % self.flags.mode)
        script_path = os.path.join(self.express.home_dir, "bin", script_name)
        assert os.path.isfile(script_path), ("Script not found: %r" % script_path)
        env["EXPRESS_MODE"] = script_path

        # express shell binary needs to be in the same directory as this script
        shell_path = os.path.join(self.express.home_dir, "bin", "express-shell")
        assert os.path.isfile(shell_path), ("Shell not found: %r" % shell_path)
        cmd = [shell_path] + list(self.args)
        logging.debug(
            "Launching the express shell using the command:\n%s",
            " \\\n\t".join(map(repr, cmd)))
        proc = subprocess.Popen(cmd, env=env)
        try:
            proc.wait()
        except subprocess.SubprocessError:
            proc.kill()

    def schema_shell(self):
        schema_shell_home = self.env.get(SCHEMA_SHELL_HOME)
        assert (schema_shell_home is not None), \
            ("Environment variable undefined: %r" % SCHEMA_SHELL_HOME)
        assert os.path.isdir(schema_shell_home), \
            ("Invalid home directory for KijiSchema shell: %r" % schema_shell_home)
        schema_shell_script = os.path.join(schema_shell_home, "bin", "kiji-schema-shell")
        assert os.path.isfile(schema_shell_script), \
            ("KijiSchema shell not found: %r" % schema_shell_script)

        env = dict(self.env)

        classpath = env.get(KIJI_CLASSPATH, "").split(":") + list(self.express.get_classpath())
        env["KIJI_CLASSPATH"] = ":".join(classpath)

        java_opts = env.get("JAVA_OPTS", "")
        # FIXME: I cannot find any trace of the Java system property "express.tmpjars"!
        # java_opts += (" -Dexpress.tmpjars=%s" % ???)

        # Relevant for KijiSchema 1.1 only and will be removed in Express 3.0:
        java_opts += " -Dorg.kiji.schema.impl.AvroCellEncoder.SCHEMA_VALIDATION=DISABLED"
        env["JAVA_OPTS"] = java_opts

        cmd = [schema_shell_script]
        logging.debug("Launching kiji-schema shell with:\n%s\with KIJI_CLASSPATH:\n%s",
                      ' \\\n\t'.join(map(repr, cmd)), '\n'.join(map(tab_indent, classpath)))
        logging.debug("Computed KIJI_CLASSPATH:")
        proc = subprocess.Popen(cmd, env=env)
        try:
            proc.wait()
        except subprocess.SubProcessError:
            proc.kill()


# --------------------------------------------------------------------------------------------------


def main(flags, args):
    cli = ExpressCLI(flags=flags, args=args, env=os.environ)
    if flags.action == 'classpath':
        return cli.classpath()
    elif flags.action == 'jar':
        return cli.jar()
    elif flags.action == 'job':
        return cli.job()
    elif flags.action == 'shell':
        return cli.shell()
    elif flags.action == 'schema-shell':
        return cli.schema_shell()
    else:
        raise Error('Unknown action: %r' % flags.action)


def init(args):
    parser = make_arg_parser()
    (flags, unparsed_args) = parser.parse_known_args(args[1:])

    try:
        log_level = parse_log_level(flags.log_level)
        setup_logging(log_level=log_level)
    except Error as err:
        print(err)
        return os.EX_USAGE

    # Run program:
    sys.exit(main(flags, unparsed_args))


if __name__ == "__main__":
    init(sys.argv)
