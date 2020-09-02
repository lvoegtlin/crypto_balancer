import argparse
import configparser
import itertools
import logging
import os
import sys
import time
from multiprocessing import Pool, cpu_count
from pathlib import Path

from crypto_balancer.ccxt_exchange import CCXTExchange
from crypto_balancer.executor import Executor
from crypto_balancer.portfolio import Portfolio
from crypto_balancer.simple_balancer import SimpleBalancer

logger = logging.getLogger(__name__)


def balancing(portfolio_config, portfolio_name, trade, force, max_orders, cancel, log_dir, mode):
    set_up_logger(log_dir, portfolio_name)

    try:
        targets = [x.split() for x in portfolio_config['targets'].split('\n')]
        targets = dict([[a, float(b)] for (a, b) in targets])
    except ValueError:
        logger.error("Targets format invalid")
        sys.exit(1)

    total_target = sum(targets.values())
    if total_target != 100:
        logger.error("Total target needs to equal 100, it is {}"
                     .format(total_target))
        return

    valuebase = portfolio_config.get('valuebase') or 'USDT'

    exchange = CCXTExchange(portfolio_config['exchange'],
                            targets.keys(),
                            portfolio_config['api_key'],
                            portfolio_config['api_secret'])

    logger.info("Connected to exchange: {}".format(exchange.name))

    if cancel:
        logger.info("Cancelling open orders...")
        for order in exchange.cancel_orders():
            logger.info("Cancelled order:", order['symbol'], order['id'])

    threshold = float(portfolio_config['threshold'])
    max_orders = int(max_orders)

    portfolio = Portfolio.make_portfolio(targets, exchange, threshold, valuebase)

    logger.info("Current Portfolio:")
    for cur in portfolio.balances:
        bal = portfolio.balances[cur]
        pct = portfolio.balances_pct[cur]
        tgt = targets[cur]
        logger.info("  {:<6s} {:<8.6f} ({:>5.2f} / {:>5.2f}%)".format(cur, bal, pct, tgt))

    logger.info("\n")
    logger.info("  Total value: {:.2f} {}".format(portfolio.valuation_quote,
                                                  portfolio.quote_currency))
    balancer = SimpleBalancer()
    executor = Executor(portfolio, exchange, balancer)
    res = executor.run(force=force,
                       trade=trade,
                       max_orders=max_orders,
                       mode=mode)

    logger.info("  Balance RMS error: {:.2g} / {:.2g}".format(
        res['initial_portfolio'].balance_rms_error,
        threshold))

    logger.info("  Balance Max error: {:.2g} / {:.2g}".format(
        res['initial_portfolio'].balance_max_error,
        threshold))

    if not portfolio.needs_balancing and not force:
        logger.info("\nNo balancing needed")
        return

    logger.info("\nBalancing needed{}:".format(" [FORCED]" if force else ""))

    logger.info("Proposed Portfolio:")
    portfolio = res['proposed_portfolio']

    if not portfolio:
        logger.info("Could not calculate a better portfolio")
        return

    for cur in portfolio.balances:
        bal = portfolio.balances[cur]
        pct = portfolio.balances_pct[cur]
        tgt = targets[cur]
        logger.info("  {:<6s} {:<8.6f} ({:>5.2f} / {:>5.2f}%)"
                    .format(cur, bal, pct, tgt))

    logger.info("  Total value: {:.2f} {}".format(portfolio.valuation_quote,
                                                  portfolio.quote_currency))
    logger.info("  Balance RMS error: {:.2g} / {:.2g}".format(
        res['proposed_portfolio'].balance_rms_error,
        threshold))

    logger.info("  Balance Max error: {:.2g} / {:.2g}".format(
        res['proposed_portfolio'].balance_max_error,
        threshold))

    total_fee = '%s' % float('%.4g' % res['total_fee'])
    logger.info("  Total fees to re-balance: {} {}"
                .format(total_fee, portfolio.quote_currency))

    logger.info("\n")
    logger.info("Orders:")
    if trade:
        for order in res['success']:
            logger.info("  Submitted: {}".format(order))

        for order in res['errors']:
            logger.info("  Failed: {}".format(order))
    else:
        for order in res['orders']:
            logger.info("  " + str(order))


def set_up_logger(log_dir, portfolio_name):
    log_path = Path(os.path.join(log_dir, portfolio_name))
    log_path.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(os.path.join(log_path, time.strftime('%d-%m-%y-%Hh-%Mm-%Ss') + '.txt'))
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)
    logger.setLevel(logging.INFO)


if __name__ == '__main__':
    p_config = configparser.ConfigParser()
    p_config.read('portfolios.ini')

    parser = argparse.ArgumentParser(
        description='Balance holdings on an exchange.')
    parser.add_argument('--trade', action="store_true",
                        help='Actually place orders')
    parser.add_argument('--force', action="store_true",
                        help='Force rebalance')
    parser.add_argument('--max_orders', default=5,
                        help='Maximum number of orders to perform in '
                             'rebalance')
    parser.add_argument('--cancel', action="store_true",
                        help='Cancel open orders first')
    parser.add_argument('--log_dir', type=str,
                        help='Path to the output folder',
                        default='logs')
    parser.add_argument('--mode', choices=['mid', 'passive', 'cheap'],
                        default='mid',
                        help='Mode to place orders')
    parser.add_argument('--portfolio', choices=p_config.sections(), nargs='+',
                        default=p_config.sections(), required=False)
    args = parser.parse_args()

    pool = Pool(processes=cpu_count())
    pool.starmap(balancing,
                 zip([p_config[p] for p in p_config.sections()],
                     p_config.sections(),
                     itertools.repeat(args.trade),
                     itertools.repeat(args.force),
                     itertools.repeat(args.max_orders),
                     itertools.repeat(args.cancel),
                     itertools.repeat(args.log_dir),
                     itertools.repeat(args.mode)
                     ))
    pool.close()
