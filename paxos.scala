import scala.actors.Actor
import scala.actors.TIMEOUT
import scala.actors.OutputChannel

object paxos {
  var proposers: Vector[Proposer] = Vector[Proposer]()
  var acceptors: Vector[Acceptor] = Vector[Acceptor]()
  var learners: Vector[Learner] = Vector[Learner]()
  var quoram = 2

  case class ProposalNumber(round: Int, pid: Int) {
    def value: Int = round * 10 + pid
  }
  case class MsgPrepare(fromId: Int, proposalNumber: ProposalNumber) {
    override def toString() = "MsgPrepare(fromId:  " + fromId + ", Proposal Number: " + proposalNumber.value + ")"
  }
  case class MsgPromise(fromId: Int, minProposal: Int, acceptedValue: Int) {
    override def toString() = "MsgPromise(fromId: " + fromId + ", minProposal: " + minProposal + ", acceptedValue: " + acceptedValue + ")"
  }
  case class MsgAccept(fromId: Int, proposalNumber: ProposalNumber, proposalValue: Int) {
    override def toString() = "MsgAccept:(fromId: " + fromId + ", proposalNumber: " + proposalNumber + ", proposalValue: " + proposalValue + ")"
  }
  case class MsgAccepted(fromId: Int, minProposal: Int) {
    override def toString() = "MsgAccepted:(fromId: " + fromId + ", minProposal: " + minProposal + ")"
  }
  object ProposerStatus extends Enumeration {
    type ProposerStatus = Value
    val PREPARE, ACCEPTPROPOSING, CHOSEN = Value
  }
  import ProposerStatus._

  class Proposer(pid: Int) extends Actor {
    def act() {
      var round = 0
      var proposalValue = pid
      var promised = 0
      var status: ProposerStatus = ProposerStatus.PREPARE

      while (true) {
        round = round + 1
        var promisedAcceptor = Set[OutputChannel[Any]]()
        var accepted = 0
        var rejected = 0
        acceptors map (acc => acc ! MsgPrepare(pid, ProposalNumber(round, pid)))
        acceptors map (acc => println(this.toString + " Send " + MsgPrepare(pid, ProposalNumber(round, pid)).toString))
        while (true) {
          receiveWithin(1000) {
            case MsgPromise(aid, minProposal, acceptedValue) => {
              if (status != ProposerStatus.PREPARE) {
                //TODO
              } else {
                println(this.toString + " Receive " + MsgPromise(aid, minProposal, acceptedValue).toString)
                if (minProposal > ProposalNumber(round, pid).value)
                  proposalValue = if (acceptedValue != -1) acceptedValue else proposalValue
              }
              promisedAcceptor = promisedAcceptor + sender
              if (promisedAcceptor.size >= quoram) {
                promisedAcceptor map (acc => acc ! MsgAccept(pid, ProposalNumber(round, pid), proposalValue))
                promisedAcceptor map (acc => println(this.toString + " Send " + MsgAccept(pid, ProposalNumber(round, pid), proposalValue).toString))
                status = ProposerStatus.ACCEPTPROPOSING
              }
            }
            case MsgAccepted(aid, proposal) => {
              if (status != ProposerStatus.ACCEPTPROPOSING) {
                //TODO
              } else {
                println(this.toString + " Receive " + MsgAccepted(aid, proposal).toString + ". Current Proposal Number: " + ProposalNumber(round, pid).value)
                if (proposal <= ProposalNumber(round, pid).value)
                  accepted = accepted + 1
                else
                  rejected = rejected + 1
                //TODO: Rejected
                if (accepted >= quoram) {
                  learners map (learner => learner ! proposalValue)
                  learners map (learner => println(this.toString + " Send " + proposalValue))
                }
              }
            }
            case TIMEOUT => {

            }
          }
        }
      }
    }
    override def toString = "Proposer[" + pid + "]"
  }
  class Acceptor(aid: Int) extends Actor {
    var minProposal = 0
    var acceptedValue = -1
    var acceptedProposal = -1

    def act() {
      loop {
        react {
          case MsgPrepare(pid, proposalNumber) => {
            println(this.toString + " Receive " + MsgPrepare(pid, proposalNumber))
            if (proposalNumber.value > minProposal)
              minProposal = proposalNumber.value
            sender ! MsgPromise(aid, acceptedProposal, acceptedValue)
            println(this.toString + " Send " + MsgPromise(aid, acceptedProposal, acceptedValue).toString)
          }
          case MsgAccept(pid, proposalNumber, proposalValue) => {
            println(this.toString + " Receive " + MsgAccept(pid, proposalNumber, proposalValue))
            if (proposalNumber.value >= minProposal) {
              minProposal = proposalNumber.value
              acceptedProposal = minProposal
              acceptedValue = proposalValue
            }
            sender ! MsgAccepted(aid, minProposal)
            println(this.toString + " Send " + MsgAccepted(aid, minProposal).toString + " with Accepted Value: " + acceptedValue)
          }
        }
      }
    }
    override def toString = "Acceptor[" + aid + "]"
  }
  class Learner extends Actor {
    def act() {
      receive {
        case msg => {
          println(this.toString + " Receive " + msg)
        }
      }
    }
    override def toString = "Learner"
  }
}

object main extends Application {
  import paxos._
  (1 to 2) map (i => proposers = proposers :+ new Proposer(i))
  (1 to 3) map (i => acceptors = acceptors :+ new Acceptor(i))
  learners = learners :+ new Learner()

  println("Start!")
  acceptors map (acc => acc.start)
  learners map (lea => lea.start)
  proposers map (pro => pro.start)
  println(proposers.size)
}