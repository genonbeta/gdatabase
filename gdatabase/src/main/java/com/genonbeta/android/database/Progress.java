package com.genonbeta.android.database;

public class Progress
{
    int mTotal;
    int mCurrent;

    public static Progress dissect(Listener listener)
    {
        Progress progress = listener.getProgress();
        if (progress == null) {
            progress = new Progress();
            listener.setProgress(progress);
        }
        return progress;
    }

    public static void addToCurrent(Listener listener, int step)
    {
        if (listener != null)
            dissect(listener).addToCurrent(step);
    }

    public static void addToTotal(Listener listener, int total)
    {
        if (listener != null)
            dissect(listener).addToTotal(total);
    }

    public static boolean call(Listener listener, int addToCurrent)
    {
        if (listener != null) {
            Progress progress = dissect(listener);
            progress.addToCurrent(addToCurrent);
            return listener.onProgressChange(progress);
        }
        return true;
    }

    public void addToCurrent(int step)
    {
        mCurrent += step;
    }

    public void addToTotal(int total)
    {
        mTotal += total;
    }

    public int getCurrent()
    {
        return mCurrent;
    }

    public int getTotal()
    {
        return mTotal;
    }

    public void setCurrent(int current)
    {
        mCurrent = current;
    }

    public void setTotal(int total)
    {
        mTotal = total;
    }

    public interface Listener
    {
        Progress getProgress();

        void setProgress(Progress progress);

        boolean onProgressChange(Progress progress);
    }

    public abstract static class SimpleListener implements Listener
    {
        private Progress mProgress;

        @Override
        public Progress getProgress()
        {
            return mProgress;
        }

        @Override
        public void setProgress(Progress progress)
        {
            mProgress = progress;
        }
    }
}
