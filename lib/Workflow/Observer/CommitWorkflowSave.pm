package Workflow::Observer::CommitWorkflowSave;

use warnings;
use strict;
use v5.14.0;
use Carp qw(croak);
use Log::Any qw( $log );

sub update {
    my ( $class, $wf, $event, $new_state ) = @_;
    return unless grep { $_ eq $event } qw( save rollback );

    my $factory = $wf->_factory;
    my $persister = $factory->get_persister( $wf->type );

    if ($event eq 'save') {
        $factory->commit_transaction;
    }
    elsif ($event eq 'rollback') {
        $factory->rollback_transaction;
    }
}

1;

__END__

=pod

=head1 NAME

Workflow::Observer::CommitWorkflowSave - Database commit after 'save' event

=head1 VERSION

This documentation describes version 2.00 of Workflow

=head1 SYNOPSIS

  # contents of workflow.xml
  <workflow>
    <type>myworkflow</type>
    ...
    <observer class="Workflow::Observer::CommitSaveWorkflow" />
  </workflow>

=head1 DESCRIPTION

This observer cooperates with C<Workflow::Persister::DBI> derived
persisters to commit workflow state to the database by committing
the database transaction after each successfully executed action
in the workflow. When the action fails to execute correctly (i.e.
the workflow captures an exception), the database transaction is
rolled back.

B<NOTE> This observer replaces the 1.x behaviour where workflows
would directly interact with their persister to commit or roll
back transactions.

=head1 SEE ALSO

L<Workflow>

=head1 COPYRIGHT

Copyright (c) 2003-2021 Chris Winters. All rights reserved.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

Please see the F<LICENSE>

=head1 AUTHORS

Please see L<Workflow>

=cut

