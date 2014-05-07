
/* Copyright (c) 2005-2013, Stefan Eilemann <eile@equalizergraphics.com>
 *
 * This file is part of Collage <https://github.com/Eyescale/Collage>
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License version 2.1 as published
 * by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

#include "init.h"

#include "global.h"
#include "node.h"
#include "socketConnection.h"

#include <lunchbox/init.h>
#include <lunchbox/os.h>
#include <lunchbox/pluginRegistry.h>

#ifdef COLLAGE_USE_MPI
#   include <mpi.h>
#endif

namespace co
{
namespace
{
static int32_t _checkVersion()
{
    static std::string version = Version::getString();
    if( version != Version::getString( ))
        LBWARN << "Duplicate DSO loading, Collage v" << version
               << " already loaded while loading v" << Version::getString()
               << std::endl;
    return 0;
}

static lunchbox::a_int32_t _initialized( _checkVersion( ));
}

bool _init( const int argc, char** argv )
{
    if( ++_initialized > 1 ) // not first
        return true;

#ifdef COLLAGE_USE_MPI

    /*  MPI library set by default the MPI_THREAD_SINGLE level of thread
        support. Collage is a multithreaded library, so, the required level of
        thread support should be MPI_THREAD_SERIALIZED at least.

        NOTE:
        Be aware that MPI_THREAD_MULTIPLE is only lightly tested and likely
        still has some bugs. Please, refer the below links:
        https://www.open-mpi.org/faq/?category=supported-systems#thread-support
        https://www.open-mpi.org/doc/v1.4/man3/MPI_Init_thread.3.php

        MPI_Init_thread and MPI_Finalize:
        Should only be called once.
        Should only be called by a single thread
        Both should be called by the same thread, known as the main thread.

        Here, the MPI library is initialized and requested for
        MPI_THREAD_MULTIPLE level of thread support. To make the library safe,
        if the thread support is not at least MPI_THREAD_SERIALIZED, MPI
        communications will not be allowed.
    */

    int threadSupportProvided = -1;
    if( MPI_SUCCESS != MPI_Init_thread( (int*) &argc, &argv,
                                       MPI_THREAD_MULTIPLE,
                                       &threadSupportProvided ) )
    {
        LBERROR << "Error at initialization MPI library" << std::endl;
        return false;
    }

    switch( threadSupportProvided )
    {
        case MPI_THREAD_SINGLE:
            LBINFO << "MPI thread support provided: MPI_THREAD_SINGLE"
                   << std::endl;
            break;
        case MPI_THREAD_FUNNELED:
            LBINFO << "MPI thread support provided: MPI_THREAD_FUNNELED"
                   << std::endl;
            break;
        case MPI_THREAD_SERIALIZED:
            LBINFO << "MPI thread support provided: MPI_THREAD_SERIALIZED"
                   << std::endl;
            break;
        case MPI_THREAD_MULTIPLE:
            LBINFO << "MPI thread support provided: MPI_THREAD_MULTIPLE"
                   << std::endl;
            break;
        default:
            LBERROR << "MPI thread support provided: unknown" << std::endl;
            return false;
    }

    if ( threadSupportProvided == MPI_THREAD_SERIALIZED ||
        threadSupportProvided == MPI_THREAD_MULTIPLE )
            co::Global::allowMPI();
#endif

    if( !lunchbox::init( argc, argv ))
        return false;

    // init all available plugins
    lunchbox::PluginRegistry& plugins = Global::getPluginRegistry();
    plugins.addLunchboxPlugins();
    plugins.addDirectory( "/opt/local/lib" ); // MacPorts
    plugins.init();

#ifdef _WIN32
    WORD    wsVersion = MAKEWORD( 2, 0 );
    WSADATA wsData;
    if( WSAStartup( wsVersion, &wsData ) != 0 )
    {
        LBERROR << "Initialization of Windows Sockets failed"
                << lunchbox::sysError << std::endl;
        return false;
    }
#endif

    return true;
}

bool exit()
{
    if( --_initialized > 0 ) // not last
        return true;
    LBASSERT( _initialized == 0 );

#ifdef _WIN32
    if( WSACleanup() != 0 )
    {
        LBERROR << "Cleanup of Windows Sockets failed"
                << lunchbox::sysError << std::endl;
        return false;
    }
#endif

    // de-initialize registered plugins
    lunchbox::PluginRegistry& plugins = Global::getPluginRegistry();
    plugins.exit();

#ifdef COLLAGE_USE_MPI
    if( MPI_SUCCESS != MPI_Finalize() )
    {
        LBERROR << "Error at finalizing MPI library" << std::endl;
        return false;
    }
#endif

    return lunchbox::exit();
}

}
