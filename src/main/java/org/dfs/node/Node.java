package org.dfs.node;

import org.dfs.wireformats.Event;

public interface Node {
    void onEvent(Event event);
}
