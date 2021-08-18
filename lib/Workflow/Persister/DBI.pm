package Workflow::Persister::DBI;

use warnings;
use strict;
use 5.006;
use parent qw( Workflow::Persister );
use DateTime;
use DateTime::Format::Strptime;
use DBI;
use Workflow::Exception qw( configuration_error persist_error );
use Workflow::Persister::RandomId;
use Workflow::Persister::DBI::AutoGeneratedId;
use Workflow::Persister::DBI::SequenceId;
use Carp qw(croak);
use Syntax::Keyword::Try;

$Workflow::Persister::DBI::VERSION = '1.56';

my @FIELDS = qw( _wf_fields _hist_fields handle dsn user password driver
    workflow_table history_table date_format parser autocommit);
__PACKAGE__->mk_accessors(@FIELDS);


sub init {
    my ( $self, $params ) = @_;
    $self->SUPER::init($params);

# Default to old date format if not provided so we don't break old configurations.
    $self->date_format('%Y-%m-%d %H:%M');

    # Default to autocommit on for backward compatibility.
    $self->autocommit(1);

    # Load user-provided values from config.
    for (qw( dsn user password date_format autocommit )) {
        $self->$_( $params->{$_} ) if ( defined $params->{$_} );
    }
    $self->handle($self->create_handle);
    my $driver
        = $self->handle ? $self->handle->{Driver}->{Name} : ($params->{driver} || '');
    $self->log->debug( "Pulled driver '$driver' from DBI DSN" );
    $self->driver($driver);
    $self->assign_generators( $params, $driver );
    $self->log->info(
        "Assigned workflow generator '",
        ref( $self->workflow_id_generator ),
        "'; ",
        "history generator '",
        ref( $self->history_id_generator ),
        "'"
    );
    $self->assign_tables($params);
    $self->log->info(
        "Assigned workflow table '",
        $self->workflow_table, "'; ", "history table '",
        $self->history_table, "'"
    );

    my $parser
        = DateTime::Format::Strptime->new( pattern => $self->date_format );
    $self->parser($parser);
}

sub create_handle {
    my ($self, $params) = @_;
    unless ( $self->dsn ) {
        configuration_error "DBI persister configuration must include ",
            "key 'dsn' which maps to the first paramter ",
            "in the DBI 'connect()' call.";
    }
    my $dbh;
    try {
        $dbh = DBI->connect( $self->dsn, $self->user, $self->password )
            or croak "Cannot connect to database: $DBI::errstr";
    }
    catch ($error) {
        persist_error $error;
    }
    $dbh->{RaiseError} = 1;
    $dbh->{PrintError} = 0;
    $dbh->{ChopBlanks} = 1;
    $dbh->{AutoCommit} = $self->autocommit();
    $self->log->debug( "Connected to database '",
                       $self->dsn, "' and ", "assigned to persister ok" );

    return $dbh;
}

sub assign_generators {
    my ( $self, $params, $driver ) = @_;
    $self->SUPER::assign_generators($params);
    return
        if ($self->workflow_id_generator
        and $self->history_id_generator );

    my ( $wf_gen, $history_gen );
    if ( $driver eq 'Pg' ) {
        $self->log->debug("Assigning ID generators for PostgreSQL");
        ( $wf_gen, $history_gen ) = $self->init_postgres_generators($params);
    } elsif ( $driver eq 'Oracle' ) {
        $self->log->debug("Assigning ID generators for Oracle");
        ( $wf_gen, $history_gen ) = $self->init_oracle_generators($params);
    } elsif ( $driver eq 'mysql' ) {
        $self->log->debug("Assigning ID generators for MySQL");
        ( $wf_gen, $history_gen ) = $self->init_mysql_generators($params);
    } elsif ( $driver eq 'SQLite' ) {
        $self->log->debug("Assigning ID generators for SQLite");
        ( $wf_gen, $history_gen ) = $self->init_sqlite_generators($params);
    } else {
        $self->log->debug("Assigning random ID generators");
        ( $wf_gen, $history_gen ) = $self->init_random_generators($params);
    }
    $self->workflow_id_generator($wf_gen);
    $self->history_id_generator($history_gen);
}

sub init_postgres_generators {
    my ( $self, $params ) = @_;
    my $sequence_select = q{SELECT NEXTVAL( '%s' )};
    $params->{workflow_sequence} ||= 'workflow_seq';
    $params->{history_sequence}  ||= 'workflow_history_seq';
    return (
        Workflow::Persister::DBI::SequenceId->new(
            {   sequence_name   => $params->{workflow_sequence},
                sequence_select => $sequence_select
            }
        ),
        Workflow::Persister::DBI::SequenceId->new(
            {   sequence_name   => $params->{history_sequence},
                sequence_select => $sequence_select
            }
        )
    );
}

sub init_oracle_generators {
    my ( $self, $params ) = @_;
    my $sequence_select = q{SELECT %s.NEXTVAL from dual};
    $params->{workflow_sequence} ||= 'workflow_seq';
    $params->{history_sequence}  ||= 'workflow_history_seq';
    return (
        Workflow::Persister::DBI::SequenceId->new(
            {   sequence_name   => $params->{workflow_sequence},
                sequence_select => $sequence_select
            }
        ),
        Workflow::Persister::DBI::SequenceId->new(
            {   sequence_name   => $params->{history_sequence},
                sequence_select => $sequence_select
            }
        )
    );
}

sub init_mysql_generators {
    my ( $self, $params ) = @_;
    my $generator = Workflow::Persister::DBI::AutoGeneratedId->new(
        {   from_handle     => 'database',
            handle_property => 'mysql_insertid',
        }
    );
    return ( $generator, $generator );
}

sub init_sqlite_generators {
    my ( $self, $params ) = @_;
    my $generator = Workflow::Persister::DBI::AutoGeneratedId->new(
        { func_property => 'last_insert_rowid' } );
    return ( $generator, $generator );
}

sub assign_tables {
    my ( $self, $params ) = @_;
    my $wf_table   = $params->{workflow_table} || 'workflow';
    my $hist_table = $params->{history_table}  || 'workflow_history';
    $self->workflow_table($wf_table);
    $self->history_table($hist_table);
}

########################################
# PERSISTENCE IMPLEMENTATION

sub create_workflow {
    my ( $self, $wf ) = @_;

    $self->_init_fields();
    my @wf_fields = @{ $self->_wf_fields };
    my @fields    = @wf_fields[ 1, 2, 3 ];
    my @values    = (
        $wf->type,
        $wf->state,
        DateTime->now( time_zone => $wf->time_zone() )
            ->strftime( $self->date_format() ),
    );
    my $dbh = $self->handle;

    my $id = $self->workflow_id_generator->pre_fetch_id($dbh);
    if ($id) {
        push @fields, $wf_fields[0];
        push @values, $id;
        $self->log->debug("Got ID from pre_fetch_id: $id");
    }
    my $sql = 'INSERT INTO %s ( %s ) VALUES ( %s )';

    $sql = sprintf $sql,
        $self->handle->quote_identifier( $self->workflow_table ),
        join( ', ', @fields ),
        join( ', ', map {'?'} @values );

    if ( $self->log->is_debug ) {
        $self->log->debug("Will use SQL: $sql");
        $self->log->debug( "Will use parameters: ", join ', ', @values );
    }

    my ($sth);
    try {
        $sth = $dbh->prepare($sql);
        $sth->execute(@values);
    }
    catch ($error) {
        persist_error "Failed to create workflow: $error";
    }
    unless ($id) {
        $id = $self->workflow_id_generator->post_fetch_id( $dbh, $sth );
        unless ($id) {
            persist_error "No ID found using generator '",
                ref( $self->workflow_id_generator ), "'";
        }
    }
    $sth->finish;

    $wf->id($id);
    return $id;
}

sub fetch_workflow {
    my ( $self, $wf_id ) = @_;
    $self->_init_fields();
    my $sql = q{SELECT %s, %s FROM %s WHERE %s = ?};
    my @wf_fields = @{ $self->_wf_fields };
    $sql = sprintf $sql,
        $wf_fields[2], $wf_fields[3],
        $self->handle->quote_identifier( $self->workflow_table ),
        $wf_fields[0];

    if ( $self->log->is_debug ) {
        $self->log->debug("Will use SQL: $sql");
        $self->log->debug("Will use parameters: $wf_id");
    }

    my ($sth);
    try {
        $sth = $self->handle->prepare($sql);
        $sth->execute($wf_id);
    }
    catch ($error) {
        persist_error "Cannot fetch workflow: $error";
    }
    my $row = $sth->fetchrow_arrayref;
    return undef unless ($row);

    return {
        state       => $row->[0],
        last_update => $self->parser->parse_datetime( $row->[1] ),
    };
}

sub update_workflow {
    my ( $self, $wf ) = @_;
    $self->_init_fields();
    my $sql = q{UPDATE %s SET %s = ?, %s = ? WHERE %s = ?};
    my @wf_fields = @{ $self->_wf_fields };
    $sql          = sprintf $sql,
        $self->handle->quote_identifier( $self->workflow_table ),
        $wf_fields[2], $wf_fields[3], $wf_fields[0];
    my $update_date = DateTime->now( time_zone => $wf->time_zone() )
        ->strftime( $self->date_format() );

    if ( $self->log->is_debug ) {
        $self->log->debug("Will use SQL: $sql");
        $self->log->debug( "Will use parameters: ",
            join ', ', $wf->state, $update_date, $wf->id );
    }

    my ($sth);
    try {
        $sth = $self->handle->prepare($sql);
        $sth->execute( $wf->state, $update_date, $wf->id );
    }
    catch ($error) {
        persist_error $error;
    }
    $self->log->info( "Workflow ", $wf->id, " updated ok" );
}

sub create_history {
    my ( $self, $wf, @history ) = @_;
    $self->_init_fields();

    my $dbh       = $self->handle;
    my $generator = $self->history_id_generator;
    foreach my $h (@history) {
        next if ( $h->is_saved );
        my $id     = $generator->pre_fetch_id($dbh);
        my @hist_fields = @{ $self->_hist_fields };
        my @fields      = @hist_fields[ 1 .. 6 ];
        my @values      = (
            $wf->id, $h->action, $h->description, $h->state, $h->user,
            $h->date->strftime( $self->date_format() ),
        );
        if ($id) {
            push @fields, $hist_fields[0];
            push @values, $id;
        }
        my $sql = 'INSERT INTO %s ( %s ) VALUES ( %s )';

        $sql = sprintf $sql, $dbh->quote_identifier( $self->history_table ),
            join( ', ', @fields ), join( ', ', map {'?'} @values );
        if ( $self->log->is_debug ) {
            $self->log->debug("Will use SQL: $sql");
            $self->log->debug( "Will use parameters: ", join ', ', @values );
        }

        my ($sth);
        try {
            $sth = $dbh->prepare($sql);
            $sth->execute(@values);
        }
        catch ($error) {
            persist_error $error;
        }
        unless ($id) {
            $id = $self->history_id_generator->post_fetch_id( $dbh, $sth );
            unless ($id) {
                persist_error "No ID found using generator '",
                    ref( $self->history_id_generator ), "'";
            }
        }
        $h->id($id);
        $h->set_saved();
        $self->log->info( "Workflow history entry ", $id, " created ok" );
    }
    return @history;
}

sub fetch_history {
    my ( $self, $wf ) = @_;
    $self->_init_fields();

    my $sql = qq{SELECT %s FROM %s WHERE %s = ? ORDER BY %s DESC};
    my @hist_fields    = @{ $self->_hist_fields };
    my $history_fields = join ', ', @hist_fields;
    $sql = sprintf $sql, $history_fields,
        $self->handle->quote_identifier($self->history_table),
        $hist_fields[1], $hist_fields[6];

    if ( $self->log->is_debug ) {
        $self->log->debug("Will use SQL: $sql");
        $self->log->debug( "Will use parameters: ", $wf->id );
    }

    my ($sth);
    try {
        $sth = $self->handle->prepare($sql);
        $sth->execute( $wf->id );
    }
    catch ($error) {
        $self->log->error("Caught error fetching workflow history: $error");
        persist_error $error;
    }
    $self->log->debug("Prepared and executed ok");

    my @history = ();
    while ( my $row = $sth->fetchrow_arrayref ) {
        $self->log->debug("Fetched history object '$row->[0]'");
        push @history, {
            id          => $row->[0],
            workflow_id => $row->[1],
            action      => $row->[2],
            description => $row->[3],
            state       => $row->[4],
            user        => $row->[5],
            date        => $self->parser->parse_datetime( $row->[6] ),
        };
    }
    $sth->finish;
    return @history;
}

sub commit_transaction {
    my ( $self, $wf ) = @_;
    if ( not $self->autocommit() ) {
        try {
            $self->handle->commit();
        }
        catch ($error) {
            $self->log->error("Caught error committing transaction: $error");
            persist_error $error;
        }
        $self->log->debug('Committed transaction.');
    }
}

sub rollback_transaction {
    my ( $self, $wf ) = @_;
    if ( not $self->autocommit() ) {
        try {
            $self->handle->rollback();
        }
        catch ($error) {
            $self->log->error("Caught error rolling back transaction: $error");
            persist_error $error;
        }
        $self->log->debug('Rolled back transaction.');
    }
    else {
        $self->log->warn(
            'Transaction NOT rolled back due to "autocommit" being enabled.'
            );
    }
}

##########
# FIELDS

# Allow subclasses to override the fieldnames

sub _init_fields {
    my ($self) = @_;
    unless ( $self->_wf_fields ) {
        $self->_wf_fields(
            [
             map {
                 $self->handle->quote_identifier($_)
             } $self->get_workflow_fields()
            ]);
    }
    unless ( $self->_hist_fields ) {
        $self->_hist_fields(
            [
             map {
                 $self->handle->quote_identifier($_)
             } $self->get_history_fields()
            ]);
    }
}

sub get_workflow_fields {
    return qw( workflow_id type state last_update );
}

sub get_history_fields {
    return qw( workflow_hist_id workflow_id
        action description state
        workflow_user history_date );
}

1;

__END__

=pod

=head1 NAME

Workflow::Persister::DBI - Persist workflow and history to DBI database

=head1 VERSION

This documentation describes version 1.56 of this package

=head1 SYNOPSIS

 <persister name="MainDatabase"
            class="Workflow::Persister::DBI"
            dsn="DBI:mysql:database=workflows"
            user="wf"
            password="mypass"/>

 <persister name="BackupDatabase"
            class="Workflow::Persister::DBI"
            dsn="DBI:Pg:dbname=workflows"
            user="wf"
            password="mypass"
            date_format="%Y-%m-%d %H:%M"
            autocommit="0"
            workflow_table="wf"
            workflow_sequence="wf_seq"
            history_table="wf_history"
            history_sequence="wf_history_seq"/>

 <persister name="OtherDatabase"
            class="My::Persister::DBHFromElsewhere"
            driver="mysql"
            />


=head1 DESCRIPTION

Main persistence class for storing the workflow and workflow history
records to a DBI-accessible datasource.

=head2 Subclassing: Getting handle from elsewhere

A common need to create a subclass is to use a database handle created
with other means. For instance, OpenInteract has a central
configuration file for defining datasources, and the datasource will
be available in a predictable manner. So we can create a subclass to
provide the database handle on demand from the C<CTX> object available
from everywhere. A sample implementation is below. (Note that in real
life we would just use SPOPS for this, but it is still a good
example.)

 package Workflow::Persister::DBI::OpenInteractHandle;

 use strict;
 use parent qw( Workflow::Persister::DBI );
 use OpenInteract2::Context qw( CTX );

 my @FIELDS = qw( datasource_name );
 __PACKAGE__->mk_accessors( @FIELDS );

 # override parent method, assuming that we set the 'datasource'
 # parameter in the persister declaration

 sub init {
    my ( $self, $params ) = @_;
    $self->datasource_name( $params->{datasource} );
    my $ds_config = CTX->lookup_datasource_config( $self->datasource_name );

    # delegate the other assignment tasks to the parent class
    $params->{driver} = $ds_config->{driver_name};
    $self->SUPER::init( $params );
 }

 # suppress the parent from trying to connect to the database
 sub create_handle { return undef; }

 sub handle {
     my ( $self ) = @_;
     return CTX->datasource( $self->datasource_name );
 }

=head2 Subclassing: Changing fieldnames

Earlier versions of Workflow used the field 'user' to record in the
history the user making a state change or comment. Unfortunately
'user' is a reserved word in our favorite database,
PostgreSQL. (Oops.) So in addition to changing the field to an
assuredly-unreserved word (workflow_user), we made the fieldnames
customizable by subclasses.

Just override either or both of the methods:

=head3 get_workflow_fields()

Return list of fields in this order:

  workflow_id, type, state, last_update

=head3 get_history_fields()

Return list of fields in this order:

  workflow_hist_id, workflow_id, action, description,
  state, workflow_user, history_date

Note that we may cache the results, so don't try and do anything weird
like change the fieldnames based on the workflow user or something...

=head1 METHODS

=head2 Public Methods

All public methods are inherited from L<Workflow::Persister>.

=head2 Private Methods

=head3 init( \%params )

Initializes the the instance by setting the connection parameters
and calling C<create_handle>. You are only required to provide 'dsn',
which is the full DBI DSN you normally use as the first argument
to C<connect()>.

You can set these parameters in your persister configuration file and
they will be passed to init.

You may also use:

=over 4

=item B<user>

Name of user to login with.

=item B<password>

Password for C<user> to login with.

=item B<date_format>

Date format to use when working with the database. Accepts a format string
that can be processed by the DateTime module. See
L<DateTime's strftime Patterns|https://metacpan.org/pod/DateTime#strftime-Patterns>
for the format options.

The default is '%Y-%m-%d %H:%M' for backward compatibility.

=item B<autocommit>

0 or 1 to turn autocommit off or on for the database handle.

Setting autocommit to off will run Workflow with transactions. If there is
a failure somewhere and the persister supports it, Workflow will attempt
to roll back all database activity in the current transaction.

If you turn autocommit off, you must still
commit transactions for L<Workflow::Persister::DBI::ExtraData> yourself. Also,
if you are sharing the database handle, you must be careful to not pass control
to the workflow engine with pending transactions as they will be committed if
the workflow actions are successful.

The default autocommit value for the database handle is on.

=item B<workflow_table>

Table to use for persisting workflow. Default is 'workflow'.

=item B<history_table>

Table to use for persisting workflow history. Default is
'workflow_history'.

=back

You may also use parameters for the different types of ID
generators. See below under the C<init_*_generator> for the necessary
parameters for your database.

In addition to creating a database handle we parse the C<dsn> to see
what driver we are using to determine how to generate IDs. We have the
ability to use automatically generated IDs for PostgreSQL, MySQL, and
SQLite. If your database is not included a randomly generated ID will
be used. (Default length of 8 characters, which you can modify with a
C<id_length> parameter.)

You can also create your own adapter for a different type of
database. Just check out the existing
L<Workflow::Persister::DBI::AutoGeneratedId> and
L<Workflow::Persister::DBI::SequenceId> classes for examples.

=head3 assign_generators( $driver, \%params )

Given C<$driver> and the persister parameters in C<\%params>, assign
the appropriate ID generators for both the workflow and history
tables.

Returns: nothing, but assigns the object properties
C<workflow_id_generator> and C<history_id_generator>.

=head3 assign_tables( \%params )

Assign the table names from C<\%params> (using 'workflow_table' and
'history_table') or use the defaults 'workflow' and 'workflow_history'.

Returns: nothing, but assigns the object properties C<workflow_table>
and C<history_table>.

=head3 init_postgres_generators( \%params )

Create ID generators for the workflow and history tables using
PostgreSQL sequences. You can specify the sequences used for the
workflow and history tables:

=over 4

=item B<workflow_sequence>

Sequence for the workflow table. Default: 'workflow_seq'

=item B<history_sequence>

Sequence for the workflow history table. Default:
'workflow_history_seq'

=back

=head3 init_mysql_generators( \%params )

Create ID generators for the workflow and history tables using
the MySQL 'auto_increment' type. No parameters are necessary.

=head3 init_sqlite_generators( \%params )

Create ID generators for the workflow and history tables using
the SQLite implicit increment. No parameters are necessary.

=head3 init_random_generators( \%params )

Create ID generators for the workflow and history tables using
a random set of characters. You can specify:

=over 4

=item B<id_length>

Length of character sequence to generate. Default: 8.

=back

=head3 init_oracle_generators

Create ID generators for the workflow and history tables using
the Oracle sequences. No parameters are necessary.

=head3 create_handle

Creates a database connection using DBI's C<connect> method and returns
the resulting database handle. Override this method if you want to set
different options than the hard-coded ones, or when you want to use a
handle from elsewhere.

The default implementation hard-codes these database handle settings:

    $dbh->{RaiseError} = 1;
    $dbh->{PrintError} = 0;
    $dbh->{ChopBlanks} = 1;

=head3 create_workflow

Serializes a workflow into the persistance entity configured by our workflow.

Takes a single parameter: a workflow object

Returns a single value, a id for unique identification of out serialized
workflow for possible deserialization.

=head3 fetch_workflow

Deserializes a workflow from the persistance entity configured by our workflow.

Takes a single parameter: the unique id assigned to our workflow upon
serialization (see L</create_workflow>).

Returns a hashref consisting of two keys:

=over

=item * state, the workflows current state

=item * last_update, date indicating last update

=back

=head3 update_workflow

Updates a serialized workflow in the persistance entity configured by our
workflow.

Takes a single parameter: a workflow object

Returns: Nothing

=head3 create_history

Serializes history records associated with a workflow object

Takes two parameters: a workflow object and an array of workflow history objects

Returns: provided array of workflow history objects upon success

=head3 fetch_history

Deserializes history records associated with a workflow object

Takes a single parameter: a workflow object

Returns an array of workflow history objects upon success

=head3 commit_transaction ( $wf )

Commit the transaction for a workflow if autocommit is not enabled.

Returns nothing

=head3 rollback_transaction

Rollsback the transaction for a workflow if autocommit is not enabled.

Returns nothing


=head1 SEE ALSO

=over

=item L<Workflow>

=item L<Workflow::Persister>

=item L<DBI>

=back

=head1 COPYRIGHT

Copyright (c) 2003-2021 Chris Winters. All rights reserved.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

Please see the F<LICENSE>

=head1 AUTHORS

Please see L<Workflow>

=cut
