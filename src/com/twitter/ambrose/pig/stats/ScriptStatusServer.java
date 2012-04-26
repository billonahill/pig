package com.twitter.ambrose.pig.stats;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.mortbay.jetty.webapp.WebAppContext;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collection;

/**
 * @author billg
 */
public class ScriptStatusServer implements Runnable {
    protected Log LOG = LogFactory.getLog(getClass());

    private int port;
    private PigStatsDataVizCollector statsCollector;
    private Server server;
    Thread serverThread;

    public ScriptStatusServer(PigStatsDataVizCollector statsCollector, int port) {
        this.port = port;
        this.statsCollector = statsCollector;
    }

    public void start() {
        try {
          LOG.info("Starting iris web server on port " + port);
          serverThread = new Thread(this);
          serverThread.setDaemon(false);
          serverThread.start();
        } catch (Exception e) {
          LOG.error("Could not start ScriptStatusServer", e);
        }
    }

    @Override
    public void run() {

        String pigHome = System.getenv("PIG_HOME");
        if (pigHome == null) {
          LOG.warn("PIG_HOME must be set to run iris and $PIG_HOME/contrib/iris must exist. Exiting iris.");
          return;
        }

        server = new Server(port);
        WebAppContext webappcontext = new WebAppContext();
        webappcontext.setContextPath("/");
        webappcontext.setWar(pigHome + "/contrib/iris/client");
        server.addHandler(new APIHandler());
        server.addHandler(webappcontext);
        server.setStopAtShutdown(false);

        try {
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stop() throws Exception {
        if (server != null) server.stop();
    }

    public class APIHandler extends AbstractHandler {

        @Override
        public void handle(String target, HttpServletRequest request,
                           HttpServletResponse response, int distpatch) throws IOException, ServletException {
            if (target.startsWith("/dag")) {
                response.setContentType("application/json");
                response.setStatus(HttpServletResponse.SC_OK);
                Collection<PigStatsDataVizCollector.DAGNode> nodes = statsCollector.getDagNodeNameMap().values();
                sendJson(request, response, nodes.toArray(new PigStatsDataVizCollector.DAGNode[nodes.size()]));
            } else if (target.startsWith("/events")) {
                response.setContentType("application/json");
                response.setStatus(HttpServletResponse.SC_OK);
                Integer sinceId = request.getParameter("sinceId") != null ?
                        Integer.getInteger(request.getParameter("sinceId")) : -1;
                Collection<PigStatsDataVizCollector.PigScriptEvent> events = statsCollector.getEventsSinceId(sinceId);
                sendJson(request, response, events.toArray(new PigStatsDataVizCollector.PigScriptEvent[events.size()]));
            }
            else if (target.endsWith(".html")) {
                response.setContentType("text/html");
                // this is because the next handler will be picked up here and it doesn't seem to
                // handle html well. This is jank.
            }
        }
    }

    private static void sendJson(HttpServletRequest request,
                                 HttpServletResponse response, Object object) throws IOException {
        ObjectMapper om = new ObjectMapper();
        om.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
        om.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
        response.getWriter().println(om.writeValueAsString(object));
        response.getWriter().flush();
        response.getWriter().close();
        Request base_request = (request instanceof Request) ?
                (Request)request : HttpConnection.getCurrentConnection().getRequest();
        base_request.setHandled(true);
    }
}
