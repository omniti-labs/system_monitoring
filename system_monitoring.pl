#!/usr/bin/env perl

package main;
use strict;
use warnings;

my $program = Monitoring->new();
$program->run();

exit;

package Monitoring;
use strict;
use warnings;
use English qw( -no_match_vars );
use Time::HiRes qw( time sleep );
use POSIX qw( strftime setsid );
use Getopt::Long;
use File::Spec;
use File::Path qw( mkpath );
use IO::Select;
use IO::Handle;

sub new {
    my $class = shift;
    return bless {}, $class;
}

sub run {
    my $self = shift;

    $self->read_command_line_options();

    $self->read_config();

    $self->validate_config();

    $self->daemonize();

    $self->{ 'select' } = IO::Select->new();
    $self->start_persistent_processes();

    $self->main_loop();
    return;
}

sub daemonize {
    my $self = shift;
    unless ( $self->{ 'daemonize' } ) {
        $self->handle_pidfile();
        return;
    }

    $self->handle_pidfile();

    #<<<  do not let perltidy touch this
    open( STDIN,  '<', '/dev/null' ) || die "can't read /dev/null: $!";
    open( STDOUT, '>', '/dev/null' ) || die "can't write to /dev/null: $!";
    defined( my $pid = fork() )      || die "can't fork: $!";
    if ( $pid ) {
        # parent
        sleep 1; # time for slave to rewrite pidfile
        exit
    }
    $self->write_pidfile();
    ( setsid() != -1 )               || die "Can't start a new session: $!";
    open( STDERR, '>&', \*STDOUT )   || die "can't dup stdout: $!";
    #>>>
    return;
}

sub handle_pidfile {
    my $self = shift;
    return unless $self->{ 'pidfile' };
    $self->verify_existing_pidfile();
    $self->write_pidfile();
    return;
}

sub write_pidfile {
    my $self = shift;
    return unless $self->{ 'pidfile' };
    my $pidfilename = $self->{ 'pidfile' };
    open my $pidfile, '>', $pidfilename or die "Cannot write to pidfile ($pidfilename): $OS_ERROR\n";
    print $pidfile $PID . "\n";
    close $pidfile;
    return;
}

sub verify_existing_pidfile {
    my $self = shift;
    return unless $self->{ 'pidfile' };
    my $pidfilename = $self->{ 'pidfile' };
    return unless -e $pidfilename;
    die "Pidfile ($pidfilename) exists, but is not a file?!\n" unless -f $pidfilename;
    open my $pidfile, '<', $pidfilename or die "Cannot read from pidfile ($pidfilename): $OS_ERROR\n";
    my $old_pid_line = <$pidfile>;
    close $pidfile;

    die "Bad format of pidfile ($pidfilename)!\n" unless $old_pid_line =~ m{\A(\d+)\s*\z};
    my $old_pid = $1;
    return if 0 == kill( 0, $old_pid );
    die "Old process ($old_pid) still exists!\n";
}

sub main_loop {
    my $self = shift;
    while ( 1 ) {
        $self->{ 'current_time' } = time();
        $self->update_logger_filehandles();

        my $timeout = $self->calculate_timeout();
        my @ready   = $self->{ 'select' }->can_read( $timeout );
        for my $fh ( @ready ) {
            $self->handle_read( $fh );
        }
        $self->start_periodic_processes();
    }
}

sub handle_read {
    my $self = shift;
    my $fh   = shift;

    my $C;
    for my $tmp ( $self->checks ) {
        next unless $tmp->{ 'input' };
        my $tmp_fh = $tmp->{ 'input' };
        next if "$tmp_fh" ne "$fh";    # Stringified reference to io handle
        $C = $tmp;
        last;
    }
    die "Data from unknown input?! It shouldn't *ever* happen\n" unless $C;

    my $read_data = '';
    while ( 1 ) {
        my $buffer;
        my $read_bytes = sysread( $fh, $buffer, 8192 );
        $read_data .= $buffer;
        last if 8192 > $read_bytes;
    }
    $C->{ 'buffer' } .= $read_data unless $C->{ 'ignore' };

    if ( '' eq $read_data ) {
        $self->{ 'select' }->remove( $fh );
        close $fh;
        delete $C->{ 'input' };
        return unless 'periodic' eq $C->{ 'type' };
        $C->{ 'next_call' } = $self->{ 'current_time' } + $C->{ 'interval' } if $self->{ 'current_time' } < $C->{ 'next_call' };
        $C->{ 'buffer' } .= "\n" if ( defined $C->{ 'buffer' } ) && ( $C->{ 'buffer' } =~ /[^\n]\z/ );
        $self->print_log( $C ) unless $C->{ 'ignore' };
        return;
    }

    delete $C->{ 'buffer' } if $C->{ 'ignore' };
    $self->print_log( $C ) unless $C->{ 'ignore' };
    return;
}

sub print_log {
    my $self = shift;
    my $C    = shift;

    my $line_prefix = strftime( '%Y-%m-%d %H:%M:%S %Z | ', localtime( $self->{ 'current_time' } ) );
    while ( $C->{ 'buffer' } =~ s{\A([^\n]*\n)}{} ) {
        my $line = $1;
        print { $C->{ 'fh' } } $line_prefix . $line;
    }
    $C->{ 'fh' }->flush();
    return;
}

sub set_env_for_check {
    my $self = shift;
    my $C    = shift;

    for my $source ( $self, $C ) {
        next unless $source->{ 'env' };
        while ( my ( $key, $value ) = each %{ $source->{ 'env' } } ) {
            if ( 0 == length $value ) {
                delete $ENV{ $key };
            }
            else {
                $ENV{ $key } = $value;
            }
        }
    }
    return;
}

sub run_check {
    my $self    = shift;
    my $C       = shift;
    my $command = $C->{ 'exec' };

    $self->set_env_for_check( $C );

    my $mode = '-|';
    $mode = '<' if $command =~ s/\A\s*<\s*//;

    open my $fh, $mode, $command or die "Cannot open [$command] in mode [$mode]: $OS_ERROR\n";
    $self->{ 'select' }->add( $fh );
    $C->{ 'input' } = $fh;

    return;
}

sub start_periodic_processes {
    my $self = shift;
    for my $C ( $self->checks ) {
        next unless 'periodic' eq $C->{ 'type' };
        next if defined $C->{ 'input' };
        next if ( defined $C->{ 'next_call' } ) && ( $C->{ 'next_call' } > $self->{ 'current_time' } );
        $self->run_check( $C );
        $C->{ 'next_call' } = $self->{ 'current_time' } + $C->{ 'interval' };
    }
    return;
}

sub start_persistent_processes {
    my $self = shift;
    for my $C ( $self->checks ) {
        next unless 'persistent' eq $C->{ 'type' };
        $self->run_check( $C );
    }
    return;
}

sub calculate_timeout {
    my $self = shift;

    my $nearest = undef;

    for my $C ( $self->checks ) {
        next if 'persistent' eq $C->{ 'type' };
        next if defined $C->{ 'input' };
        return 0 unless defined $C->{ 'next_call' };
        if ( defined $nearest ) {
            $nearest = $C->{ 'next_call' } if $C->{ 'next_call' } < $nearest;
        }
        else {
            $nearest = $C->{ 'next_call' };
        }
    }

    $nearest = $self->{ 'current_time' } unless defined $nearest;
    my $sleep_time = $nearest - $self->{ 'current_time' };

    return $sleep_time < 0.5 ? 0.5 : $sleep_time;    # limit sleep time to 0.5s to avoid too aggresive calls.
}

sub update_logger_filehandles {
    my $self = shift;

    my $file_suffix = strftime( '-%Y-%m-%d-%H.log', localtime( $self->{ 'current_time' } ) );
    return if ( defined $self->{ 'previous-suffix' } ) && ( $self->{ 'previous-suffix' } eq $file_suffix );
    $self->{ 'previous-suffix' } = $file_suffix;

    my $directory_prefix = strftime( '%Y/%m/%d', localtime( $self->{ 'current_time' } ) );
    my $full_directory = File::Spec->catfile( $self->{ 'logdir' }, $directory_prefix );

    mkpath( [ $full_directory ], 0, oct( "750" ) ) unless -e $full_directory;

    for my $C ( $self->checks ) {
        next if $C->{ 'ignore' };

        if ( $C->{ 'fh' } ) {
            close $C->{ 'fh' };
            delete $C->{ 'fh' };
        }

        my $full_name = File::Spec->catfile( $full_directory, $C->{ 'name' } . $file_suffix );
        open my $fh, '>>', $full_name or die "Cannot write to $full_name: $OS_ERROR\n";
        $C->{ 'fh' } = $fh;

        if (   ( $C->{ 'header' } )
            && ( !-s $full_name ) )
        {

            # File is empty
            $C->{ 'buffer' } .= "\n" if $C->{ 'buffer' };
            $C->{ 'buffer' } .= $C->{ 'header' };
            $self->print_log( $C );
        }
    }

    return;
}

sub checks {
    my $self = shift;
    return @{ $self->{ 'checks' } };
}

sub validate_config {
    my $self = shift;

    die "GLOBAL.logdir was not provided in config!\n" unless defined $self->{ 'logdir' };
    die "There are no checks to be run!\n"            unless defined $self->{ 'pre_checks' };

    die "Cannot chdir to " . $self->{ 'logdir' } . ": $OS_ERROR\n" unless chdir $self->{ 'logdir' };

    my @checks = ();
    while ( my ( $check, $C ) = each %{ $self->{ 'pre_checks' } } ) {
        $C->{ 'name' } = $check;
        push @checks, $C;

        die "Bad type " . $C->{ 'type' } . " in check $check!\n" unless $C->{ 'type' } =~ m{\A(?:persistent|periodic)\z};
        next unless $C->{ 'type' } eq 'periodic';

        die "Undefined interval for check $check!\n" unless defined $C->{ 'interval' };
        die "Bad interval (" . $C->{ 'interval' } . ") in check $check!\n" unless $C->{ 'interval' } =~ m{\A[1-9]\d*\z};

        $self->process_config_vars( $C );

        if ( $C->{ 'header' } ) {
            my $header = $C->{ 'header' };
            if ( $header =~ s/^!\s*// ) {
                $header .= ' 2>&1' unless $header =~ m{\b2>};
                $self->set_env_for_check( $C );
                $header = `$header`;
            }
            $header =~ s/\s*\z/\n/;
            $C->{ 'header' } = $header;
        }

        # redirect stderr to stdout if it hasn't been redirected in the exec command itself, and it's not just file to read
        next if $C->{ 'exec' } =~ m{\A\s*<};
        next if $C->{ 'exec' } =~ m{\b2>};
        $C->{ 'exec' } .= ' 2>&1';
    }

    $self->{ 'checks' } = \@checks;
    delete $self->{ 'pre_checks' };

    return;
}

sub process_config_vars {
    my $self = shift;
    my $C    = shift;
    return unless $self->{ 'var_re' };
    my $re = $self->{ 'var_re' };

    for my $field ( qw( exec header ) ) {
        next unless $C->{ $field };
        $C->{ $field } =~ s/\@$re/$self->{'var'}->{$1}/eg;
    }
    return;
}

sub read_config {
    my $self = shift;

    my $config_file_name = $self->{ 'config_file' };

    open my $fh, '<', $config_file_name or die "Cannot open config file ($config_file_name) : $OS_ERROR\n";
    while ( my $line = <$fh> ) {
        next if $line =~ m{^\s*#};     # comment
        next if $line =~ m{^\s*\z};    # empty line
        $line =~ s{\A\s*}{};           # removing leading spaces
        $line =~ s{\s*\z}{};           # removing trailing spaces
        if ( $line =~ m{ \A GLOBAL\.(logdir|pidfile) \s* = \s* (\S.*) \z }xmsi ) {
            $self->{ lc $1 } = $2;
            next;
        }
        if ( $line =~ m{ \A GLOBAL\.env\.([^\s=]+) \s* = \s* (.*) \z }xmsi ) {
            $self->{ 'env' }->{ $1 } = $2;
            next;
        }
        if ( $line =~ m{ \A GLOBAL\.var\.([A-Za-z0-9_]+) \s* = \s* (.*) \z }xmsi ) {
            $self->{ 'var' }->{ $1 } = $2;
            next;
        }
        elsif ( $line =~ m{ \A check\.([A-Za-z0-9_]+)\.(type|exec|interval|header|ignore) \s* = \s* (\S.*) \z }xmsi ) {
            $self->{ 'pre_checks' }->{ $1 }->{ $2 } = $3;
            next;
        }
        elsif ( $line =~ m{ \A check\.([A-Za-z0-9_]+)\.env\.([^\s=]+) \s* = \s* (.*) \z }xmsi ) {
            $self->{ 'pre_checks' }->{ $1 }->{ 'env' }->{ $2 } = $3;
            next;
        }
        die "Unknown line: [ $line ]\n";
    }
    close $fh;
    return unless $self->{ 'var' };
    my @all_vars = sort { length( $b ) <=> length( $a ) } keys %{ $self->{ 'var' } };
    my $vars_as_string = join '|', @all_vars;
    my $vars_re = qr{($vars_as_string)};
    $self->{ 'var_re' } = $vars_re;
    return;
}

sub read_command_line_options {
    my $self = shift;

    my $daemonize = undef;
    exit( 1 ) unless GetOptions( 'daemonize|d' => \$daemonize );
    $self->{ 'daemonize' } = $daemonize;

    die "You have to provide name of config file! Check: perldoc $PROGRAM_NAME\n" if 0 == scalar @ARGV;
    $self->{ 'config_file' } = shift @ARGV;

    return;
}

1;

=head1 system_monitoring.pl

=head2 USAGE

system_monitoring.pl [-d] <config_file>

=head2 DESCRIPTION

system_monitoring.pl script is meant to provide single and solution for
logging system data which change more often than it's practical for systems
like cacti/nagios.

It is meant to be run on some low-privilege account, and gather the data,
which are partitioned automatically by source, and time, and stored in
simple text files.

After running, system_monitor.pl will check config, and if there are no
errors - will start processing checks.

All checks work in parallel, so there is no chance single check could lock
whole system_monitoring.pl.

=head2 Configuration file

Format of the configuration file is kept as simple as possible, to make this
script very portable - which in this particular case means: no external
(aside from core perl) dependencies.

Each line should be one of:

=over

=item * Comment (starts with #)

=item * Empty line (just white space characters)

=item * Setting

=back

Where setting line looks like:

    PARAM=value

with optional leading, trailing or around "=" whitespace.

Recognized parameters are:

=over

=item * GLOBAL.logdir - full path to log directory

=item * GLOBAL.pidfile- full path to file which should contain pid of currently running system_monitoring.pl

=item * GLOBAL.env.* - setting environment variables

=item * GLOBAL.var.* - setting variable to be used as expansion in header and exec lines. For example: GLOBAL.var.psql=/long/path/psql lets you later use check.XXX.exec=@psql. Variable names are limited to /^[A-Za-z0-9_]$/

=item * check.XXX.type - type of check with name XXX

=item * check.XXX.exec - what should be executed to get data for check XXX

=item * check.XXX.header - whenever first write to new file for given check is done, it should be printed first. If header value starts with ! it is treated (sans the ! character) as command to run
that will output header. It has to be noted, though, that it's locking call - but it's only evaluated once - at the startup of monitoring script (this is intentional to 

=item * check.XXX.interval - how often to run check XXX

=item * check.XXX.ignore - should output be ignored?

=item * check.XXX.env.* - setting environment variables

=back

There are only two supported types:

=over

=item * persistent - which means given program is to be run in background,
and whatever it will return should be logged. Such program "interval" will
be ignored.

=item * periodic - which means that given program is to be run periodically
as it will exit after returning data

=back

env parameters are used to set environment variables. You can set them
globally for all checks, via GLOBAL.env., or for any given check itself -
using check.XXX.env.

For example:

    GLOBAL.env.PGUSER=postgres

Will set environment variable PGUSER to value postgres.

If you'd want to make sure that given env variable is not set, you can use
syntax with lack of value:

    check.whatever.env.PGUSER=

"exec" parameter is simply command line, to be run via shell, that will run
the program.

If exec parameter starts with '<' character (with optional whitespace
characters after), it is treated as filename to be read, and logged.

Due to the way it is internally processed - using "<" approach makes sense
only for periodic checks - in case of permenent checks it would simply copy
the file at start of system_monitoring.pl, and ignore any changes to it
afterwards. If you'd like to have something like 'tail -f' - use tail -f.

interval is time (in seconds) how often given program (of periodic type)
should be run.

ignore is optional parameter which is checked using Perl boolean logic (any
value other than empty string or 0 ar treated as true). Since
system_monitoring doesn't let setting empty string as value for option -
it's best to not include ignore option for checks you want to log, and just
add '...ignore=1' for those that you want to ignore.

If ignore is set, system_monitoring will not log output from such check.

This is helpful to build-in compression of older logs, using for example:

    check.cleanup.type=periodic
    check.cleanup.interval=300
    check.cleanup.exec=find /var/log/monitoring -type f -name '*.log' -mmin +120 -print0 | xargs -0 gzip
    check.cleanup.ignore=1

"XXX" (name of check) can consist only of upper and lower case letters,
digits, and character _. That is it has to match regular expression:

    /\A[A-Za-z0-9_]+\z/

Output from all programs will be logged in files named:

    /logdir/YYYY/MM/DD/XXX-YYY-MM-DD-HH.log

where YYYY, MM, DD and HH are date and time parts of current (as of logging
moment) time.

HH is 0 padded 24-hour style hour.

Example configuration:

    # Global configuration, log directory
    GLOBAL.logdir=/var/tmp/monitoring

    # Logging iostat output in 10 second intervals
    check.iostat.type=persistent
    check.iostat.exec=iostat -kx 10

    # Logging "ps auxwwn" every 30 seconds.
    check.ps.type=periodic
    check.ps.exec=ps auxwwn
    check.ps.interval=30

=head2 INTERNALS

Program itself is very short:

    my $program = Monitoring->new();
    $program->run();

This creates $program as object of Monitoring class (defined in the same
file), and calls method run() on it.

=head3 METHODS

=head4 new

Just object constructor. Nothing to see there.

=head4 run

Initialization of stuff, and call to main_loop. Reads and validates config
(by calls to appropriate methods), initializes IO::Select object for
asynchronous I/O, starts persistent checks (again, using special metod), and
enters main_loop();

=head4 main_loop

The core of the program. Infinite loop, which - upon every iteration:

=over

=item * updates logging filehandles

=item * checks if there is anything to read in input filehandles (from
checks)

=item * reads whatever is to be read from checks

=item * runs new periodic checks if the time has come to do it

=back

Checking for data in input filehandles is done with timeout, which is
calculated to finish when next check will have to be run, so the program
doesn't use virtually no CPU unless there are some data to be worked on.

=head4 handle_read

Since all we get from IO::Select is filehandle to read from, this method has
first to find which check given filehandle belongs to.

Afterwards, it reads whatever is available in the filehandle. In case there
is error on the filehandle - it closes the filehandle - as it means that
output for given check ended.

Every line from check is prefixed with timestamp and logged to appropriate
logfile.

Additionally, when closing the filehandle (on error), it sets when given
check should be run next time.

=head4 run_check

Simple helper function which runs external program (or opens filehandle for
reading from file), and puts it into check data.

=head4 start_periodic_processes

Iterates over all periodic processes, checks which should be already run,
and runs them.

=head4 start_persistent_processes

Iterates over all persistent processes and runs them. This is done only
once, from run() method.

=head4 calculate_timeout

Helper function which calculates how long should main_loop() wait for data
from IO::Select before it has to run another round of
start_periodic_processes().

=head4 update_logger_filehandles

Checks if current timestamp has changed enough to require swapping files,
and if yes - closes old ones and opens new ones - making all necessary
directories to make it happen.

=head4 checks

Wrapper to be able to write:

    for my $C ( $self->checks ) {

instead of:

    for my $C ( @{ $self->{ 'checks'} } ) {

=head4 validate_config

Verifies that config values make sense, and reorganizes them into final data
structure (checks hashes in $self->{'checks'} arrayref).

=head4 read_config

Just like name suggests - reads given config to memory. Very simple parser
based on regular expressions.

=head2 LICENSE

Copyright (c) 2010,2011, OmniTI, Inc.

Permission to use, copy, modify, and distribute this software and its
documentation for any purpose, without fee, and without a written agreement
is hereby granted, provided that the above copyright notice and this
paragraph and the following two paragraphs appear in all copies.

IN NO EVENT SHALL OmniTI, Inc. BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT,
SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS,
ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
OmniTI, Inc. HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

OmniTI, Inc. SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS,
AND OmniTI, Inc. HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT,
UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

=head2 COPYRIGHT

The system_monitoring project is Copyright (c) 2010,2011 OmniTI. All rights reserved.

