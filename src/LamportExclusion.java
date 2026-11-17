import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

class Message {
    enum Type {
        REQUEST,
        REPLY,
        RELEASE
    }

    final Type type;
    final int senderId;
    final int timestamp;

    Message(Type type, int senderId, int timestamp) {
        this.type = type;
        this.senderId = senderId;
        this.timestamp = timestamp;
    }
}

class Processus extends Thread {
    int id;
    int clock = 0;
    List<Processus> autres = new ArrayList<>();
    Queue<Message> fileMessages = new ConcurrentLinkedQueue<>();
    boolean enSC = false;
    Set<Integer> reponsesRecues = new HashSet<>();
    List<Message> fileDemandes = new ArrayList<>();

    Processus(int id) {
        this.id = id;
    }

    void setAutres(List<Processus> autres) {
        this.autres = autres;
    }

    void tick() {
        clock++;
    }

    void majHorloge(int autreHorloge) {
        clock = Math.max(clock, autreHorloge) + 1;
    }

    void envoyer(Message m, Processus dest) {
        dest.recoitMessage(m);
    }

    synchronized void recoitMessage(Message m) {
        majHorloge(m.timestamp);
        fileMessages.add(m);
    }

    void broadcast(Message.Type type) {
        tick();
        Message message = new Message(type, id, clock);
        for (Processus processus : autres) {
            if (processus != this) {
                envoyer(message, processus);
            }
        }
    }

    void demandeSC() {
        Message demande;
        synchronized (this) {
            reponsesRecues.clear();
            tick();
            demande = new Message(Message.Type.REQUEST, id, clock);
            fileDemandes.removeIf(message -> message.senderId == id);
            fileDemandes.add(demande);
            System.out.printf("Processus %d demande SC à t=%d%n", id, clock);
        }

        for (Processus processus : autres) {
            if (processus != this) {
                envoyer(demande, processus);
            }
        }

        reponsesRecues.add(id);
        while (true) {
            traiterMessages();
            synchronized (this) {
                if (reponsesRecues.size() >= autres.size() && demandeEstPrioritaire()) {
                    break;
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        entrerSC();
    }

    private synchronized boolean demandeEstPrioritaire() {
        Message demandeLocale = null;
        for (Message demande : fileDemandes) {
            if (demande.senderId == id) {
                demandeLocale = demande;
                break;
            }
        }
        if (demandeLocale == null) {
            return false;
        }

        for (Message demande : fileDemandes) {
            if (demande.type == Message.Type.REQUEST
                    && (demande.timestamp < demandeLocale.timestamp
                    || (demande.timestamp == demandeLocale.timestamp
                    && demande.senderId < demandeLocale.senderId))) {
                return false;
            }
        }
        return true;
    }

    void entrerSC() {
        synchronized (this) {
            enSC = true;
            System.out.printf("==> Processus %d entre en SC (t=%d)%n", id, clock);
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            sortirSC();
        }
    }

    void sortirSC() {
        synchronized (this) {
            enSC = false;
            System.out.printf("<== Processus %d quitte SC (t=%d)%n", id, clock);
            fileDemandes.removeIf(message -> message.senderId == id);
        }
        broadcast(Message.Type.RELEASE);
    }

    void traiterMessages() {
        Message message;
        while ((message = fileMessages.poll()) != null) {
            switch (message.type) {
                case REQUEST:
                    synchronized (this) {
                        fileDemandes.add(message);
                    }
                    envoyer(new Message(Message.Type.REPLY, id, clock), getProcessus(message.senderId));
                    break;
                case REPLY:
                    synchronized (this) {
                        reponsesRecues.add(message.senderId);
                    }
                    break;
                case RELEASE:
                    int senderId = message.senderId;
                    synchronized (this) {
                        fileDemandes.removeIf(
                                demande -> demande.senderId == senderId);
                    }
                    break;
                default:
                    throw new IllegalStateException("Type de message inconnu");
            }
        }
    }

    Processus getProcessus(int id) {
        for (Processus processus : autres) {
            if (processus.id == id) {
                return processus;
            }
        }
        return null;
    }

    @Override
    public void run() {
        Random random = new Random();
        for (int i = 0; i < 3; i++) {
            try {
                Thread.sleep(500 + random.nextInt(1000));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            demandeSC();
        }

        long finService = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < finService) {
            traiterMessages();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}

public class LamportExclusion {
    public static void main(String[] args) throws InterruptedException {
        List<Processus> processus = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            processus.add(new Processus(i));
        }

        for (Processus process : processus) {
            process.setAutres(processus);
        }

        for (Processus process : processus) {
            process.start();
        }

        for (Processus process : processus) {
            process.join();
        }
    }
}
