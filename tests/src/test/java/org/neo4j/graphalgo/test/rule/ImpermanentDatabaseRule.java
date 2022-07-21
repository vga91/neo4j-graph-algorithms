package org.neo4j.graphalgo.test.rule;

import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class ImpermanentDatabaseRule extends DatabaseRule {
    private final LogProvider userLogProvider;
    private final LogProvider internalLogProvider;

    public ImpermanentDatabaseRule()
    {
        this( null );
    }

    public ImpermanentDatabaseRule( LogProvider logProvider )
    {
        this.userLogProvider = logProvider;
        this.internalLogProvider = logProvider;
    }

    @Override
    public ImpermanentDatabaseRule startLazily()
    {
        return (ImpermanentDatabaseRule) super.startLazily();
    }

    @Override
    protected DatabaseManagementServiceBuilder newFactory()
    {
        return maybeSetInternalLogProvider( maybeSetUserLogProvider( new TestDatabaseManagementServiceBuilder().impermanent() ) );
    }

    protected final TestDatabaseManagementServiceBuilder maybeSetUserLogProvider( TestDatabaseManagementServiceBuilder factory )
    {
        return ( userLogProvider == null ) ? factory : factory.setUserLogProvider( userLogProvider );
    }

    protected final TestDatabaseManagementServiceBuilder maybeSetInternalLogProvider( TestDatabaseManagementServiceBuilder factory )
    {
        return ( internalLogProvider == null ) ? factory : factory.setInternalLogProvider( internalLogProvider );
    }
}
