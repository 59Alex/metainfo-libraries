package io.github.alex59.metainfo.jolt.model;



import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.event.Level;

@Getter
@Setter
@AllArgsConstructor
public class JoltLoggingProperties {

    Boolean enabled;
    Level level;

    public boolean isInfoEnabled() {
        return isLevel(Level.INFO) || isDebugEnabled();
    }

    public boolean isDebugEnabled() {
        return isLevel(Level.DEBUG) || isTraceEnabled();
    }

    public boolean isTraceEnabled() {
        return isLevel(Level.TRACE) || isDetailedEnabled();
    }

    public boolean isDetailedEnabled() {
        return false;
    }

    private boolean isLevel(Level level) {
        return level.equals(this.level);
    }

}
