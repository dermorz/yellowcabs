from click.testing import CliRunner

from yellowcabs.cli import average_trip_duration


def test_average_trip_duration():
    runner = CliRunner()
    result = runner.invoke(average_trip_duration, ["2019-02"])
    assert result.exit_code == 0
    assert result.output == "The average trip duration in 02/2019 was 579 seconds.\n"
