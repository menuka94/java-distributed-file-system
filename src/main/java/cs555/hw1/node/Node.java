package cs555.hw1.node;

import cs555.hw1.wireformats.Event;

public interface Node {
    void onEvent(Event event);
}
